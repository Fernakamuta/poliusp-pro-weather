import os
import requests
import pandas as pd
from google.cloud import storage
from dotenv import load_dotenv
load_dotenv()


def get_top_cities_weather(api_key):
    """Makes a request to the accuweather api."""
    accuweather_host = "http://dataservice.accuweather.com/locations/v1/"
    accuweater_top_cities_endpoint = "topcities/50"
    res = requests.get(url = accuweather_host + accuweater_top_cities_endpoint, params={"apikey": api_key}).json()
    return res


def parse_dataframe(weather_res):
    """Parses response into a dataframe."""
    cities_info = []

    for w in weather_res:
        city_dict = {
            "key": w["Key"],
            "name": w["EnglishName"],
            "country": w["Country"]["EnglishName"],
            "region": w["Region"]["EnglishName"],
            "latitude": w["GeoPosition"]["Latitude"],
            "longitude": w["GeoPosition"]["Longitude"],
            "elevation": w["GeoPosition"]["Elevation"]["Metric"]["Value"]
        }

        cities_info.append(city_dict)
    return pd.DataFrame(cities_info)


def upload_to_storage(bucket_name, source_file_name, destination_blob_name):
    """Uploads a file to the bucket."""
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    blob.upload_from_filename(source_file_name)

    print(f"File {source_file_name} uploaded to {destination_blob_name}.")

if __name__ == "__main__":
    bucket_name = "poliusp-tmp"
    source_file_name = "weather.csv"
    destination_blob_name = "weather/weather.csv"

    accuweather_key = os.getenv('ACCUWEATHER_KEY')

    weather_res = get_top_cities_weather()
    weather_df = parse_dataframe(weather_res)
    weather_df.to_csv(source_file_name)

    upload_to_storage(bucket_name, source_file_name, destination_blob_name)

    print(weather_df)
