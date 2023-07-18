import httpx
from prefect import flow, task
from prefect.filesystems import LocalFileSystem, S3
import pandas as pd
from datetime import timedelta

@task
def save_weather(weather):
    print(weather.json()["hourly"]["temperature_2m"])
    df = pd.DataFrame(weather.json()["hourly"]["temperature_2m"])
    df.to_csv('test.csv', sep='\t', encoding='utf-8')

    return True

@task(retries=5, persist_result=True, result_storage_key="temperature.json")
def fetch_weather(lat: float, lon: float):
    base_url = "https://api.open-meteo.com/v1/forecast/"
    weather = httpx.get(
        base_url,
        params=dict(latitude=lat, longitude=lon, hourly="temperature_2m,dewpoint_2m",temperature_unit="fahrenheit"),
    )
    return weather

@flow()
def do_all_the_things(lat=33.7, lon=-117.7):
    weather = fetch_weather(lat, lon)
    most_recent_temp = float(weather.json()["hourly"]["temperature_2m"][0])
    most_recent_dewpoint = float(weather.json()["hourly"]["dewpoint_2m"][0])
    print(f"Most recent temp F: {most_recent_temp} degrees")
    print(f"Most recent depoint F: {most_recent_dewpoint} degrees")
    save_weather(weather)
    return weather


if __name__ == "__main__":
    do_all_the_things(33.7,-117.7)