import argparse
import json
import logging

import apache_beam as beam
import pandas as pd
from apache_beam.options.pipeline_options import PipelineOptions
from google.cloud import storage
from smart_open import open
import os

""" sample json that we are going to be parsing through our program, store this in a gcs bucket and then
run this script which converts : dataset["properties"]["periods"] has a series of json that looks like this
{
    number	:	1
    name	:	Today
    startTime	:	2022-11-09T10:00:00-05:00
    endTime	:	2022-11-09T18:00:00-05:00
    isDaytime	:	true
    temperature	:	56
    temperatureUnit	:	F
    temperatureTrend	:	falling
    windSpeed	:	3 to 7 mph
    windDirection	:	E
    icon	:	https://api.weather.gov/icons/land/day/few?size=medium
    shortForecast	:	Sunny
    detailedForecast	:	Sunny. High near 56, with temperatures falling to around 52 in the afternoon. East wind 3 to 7 mph.
}
"""

class ReadFile(beam.DoFn):
    def __init__(self, input_path):
        self.input_path = input_path

    def start_bundle(self):
        from google.cloud import storage
        self.client = storage.Client()

    def process(self, something):
        with open(self.input_path) as fin:
            ss = fin.read()
            data = json.loads(ss) # -> returns python dictionary
            weather_details = data["properties"]["periods"]
        
            Temps = []
            for line in weather_details:
                Day = line['name'] # string
                if_daytime = line['isDaytime'] # boolean
                temp_value = line['temperature'] # Int
                temp_unit = line['temperatureUnit'] # string
                temp_trend = line['temperatureTrend'] # string
                wind_dir = line['windDirection'] # string
                short = line['shortForecast'] # string
                detailed = line['detailedForecast'] # string
                Temps.append([Day,if_daytime,temp_value,temp_unit,temp_trend,wind_dir,short,detailed])

        yield Temps

class WriteCSVFile(beam.DoFn):

    def __init__(self, bucket_name):
        self.bucket_name = bucket_name

    def start_bundle(self):
        from google.cloud import storage
        self.client = storage.Client()

    def process(self, mylist):
        df = pd.DataFrame(mylist, columns={'Day': str, 'if_daytime': bool, 'temp_value': int, 'temp_unit': str, 'temp_trend': str,'wind_dir':str,'short':str,'detailed':str})

        bucket = self.client.get_bucket(self.bucket_name)

        bucket.blob(f"csv_exports.csv").upload_from_string(df.to_csv(index=False), 'text/csv')

class DataflowOptions(PipelineOptions):

    @classmethod
    def _add_argparse_args(cls,parser):
        parser.add_argument('--input_path', type=str, default='gs://fy_weather/weather.json') #change bucket name here in the 'gs://...' format
        parser.add_argument('--output_bucket', type=str, default='gs://fy_weather/csv-weather.csv') #change bucket name here in similar format to <--

def run(argv=None):
    parser = argparse.ArgumentParser()
    _, pipeline_args = parser.parse_known_args(argv)

    pipeline_options = PipelineOptions(pipeline_args)
    dataflow_options = pipeline_options.view_as(DataflowOptions)

    with beam.Pipeline(options=pipeline_options) as pipeline:
        (pipeline
        | 'Start' >> beam.Create([None])
        | 'Read JSON' >> beam.ParDo(ReadFile(dataflow_options.input_path))
        | 'Write CSV' >> beam.ParDo(WriteCSVFile(dataflow_options.output_bucket))
        )

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
