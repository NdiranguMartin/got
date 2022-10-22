from unittest import result
import requests
from datetime import date, timedelta

today = date.today()
yesterday = today - timedelta(1)
print(today, yesterday)

AEROAPI_KEY = 'ep9tNKyUUkJD0fZ966OyRzk1JXSEbbc4'

AEROAPI = requests.Session()
AEROAPI.headers.update({"x-apikey": AEROAPI_KEY})

results = AEROAPI.get("https://aeroapi.flightaware.com/aeroapi/airports/HKJK/flights/arrivals?start=2022-10-21&end=2022-10-22&max_pages=10")

print(results.json())