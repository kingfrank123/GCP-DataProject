# https://www.weather.gov/documentation/services-web-api
import requests
import json
lat,long = 40.69868,-73.70545
response1 = requests.get("https://api.weather.gov/points/{lat},{long}".format(lat=lat,long=long))
response1.json().keys() # -> gets me all the keys
data1 = response1.json()
# data1["properties"]["forecast"] -> gets me the forecast website
response2 = requests.get(data1["properties"]["forecast"])
response2.json().keys() # -> dict_keys(['@context', 'type', 'geometry', 'properties'])
data2 = response2.json()
#details = data2["properties"]["periods"] # -> next weeks weather details and is a list
# writing to a json file to upload onto gcs
with open("FY_weather/weather.json", "w") as outfile:
    json.dump(data2, outfile)