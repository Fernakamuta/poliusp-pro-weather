import os
import time
import requests

from dotenv import load_dotenv
load_dotenv()

def get_current_weather_conditions(location_key, api_key):
    """Makes a request to the accuweather api."""
    accuweather_endpoint = f"http://dataservice.accuweather.com/currentconditions/v1/{location_key}"
    res = requests.get(url=accuweather_endpoint, params={"apikey": api_key}).json()
    return res


def run_script():
    # your script logic here
    print("Script executed")

while True:
    accuweather_key = os.getenv('ACCUWEATHER_KEY')
    location_keys = os.getenv('LOCATION_KEYS').split(',')

    for location_key in location_keys:
        current_weather_conditions = get_current_weather_conditions(location_key, accuweather_key)
        print(current_weather_conditions)


    print(accuweather_key)
    run_script()
    time.sleep(0.1 * 60)  # x is the number of minutes
