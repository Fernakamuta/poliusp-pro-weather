import os
import time
import requests
import pandas as pd

from dotenv import load_dotenv
load_dotenv()

def get_current_weather_conditions(location_key, api_key):
    """Makes a request to the current conditions accuweather api."""
    accuweather_endpoint = f"http://dataservice.accuweather.com/currentconditions/v1/{location_key}"
    res = requests.get(url=accuweather_endpoint, params={"apikey": api_key, "details": True, "language": "pt-br"}).json()
    return res


def get_forecast_weather_conditions(location_key, api_key):
    """Makes a request to the forecast accuweather api."""
    accuweather_endpoint = f"http://dataservice.accuweather.com/forecasts/v1/daily/1day/{location_key}"
    res = requests.get(url=accuweather_endpoint, params={"apikey": api_key, "language": "pt-br"}).json()
    return res


def upload_at_gcs(df):
    import pandas as pd
    from io import StringIO
    from datetime import datetime
    from google.cloud import storage

    PROJECT_ID = 'poli-usp-pro-cloud-computing'
    BUCKET_NAME = 'poliusp-demo'

    storage= storage.Client(project=PROJECT_ID)
    bucket = storage.get_bucket(bucket_or_name=BUCKET_NAME)


    now = datetime.now()
    blob_name = now.strftime("%Y%m%d_%H%M%S") + ".csv"
    df.to_csv(blob_name)

    blob = bucket.blob("weather/" + blob_name)
    blob.upload_from_filename(blob_name)
    print("CSV Saved")


while True:
    accuweather_key = os.getenv('ACCUWEATHER_KEY')
    location_keys = os.getenv('LOCATION_KEYS').split(',')

    weather_data = []

    for location_key in location_keys:
        try:
            current_conditions_res = get_current_weather_conditions(location_key, accuweather_key)
            forecast_res = get_forecast_weather_conditions(location_key, accuweather_key)

            payload = {
                "time": current_conditions_res[0]["LocalObservationDateTime"],
                "location_key": location_key,
                "uv_index": current_conditions_res[0]["UVIndex"],
                "temperature": current_conditions_res[0]["Temperature"]["Metric"]["Value"],
                "real_feel_temperature": current_conditions_res[0]["RealFeelTemperature"]["Metric"]["Value"],
                "forecast_text": forecast_res["Headline"]["Text"],
                "minimum": round((forecast_res["DailyForecasts"][0]["Temperature"]["Minimum"]["Value"] - 32) * 5 / 9, 1),  # Convert to Celsius
                "maximum": round((forecast_res["DailyForecasts"][0]["Temperature"]["Maximum"]["Value"] - 32) * 5 / 9, 1)   # Convert to Celsius
            }

            weather_data.append(payload)


        except Exception as e:
            print(f"Error processing location {location_key}: {e}")

    df_weather = pd.DataFrame(weather_data)
    print(df_weather.shape)
    upload_at_gcs(df_weather)
    time.sleep(0.1 * 60)  # x is the number of minutes
