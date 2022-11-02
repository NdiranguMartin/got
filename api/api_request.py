import json
from unittest import result
import requests
from datetime import date, timedelta
from dotenv import load_dotenv
import os
from snowflake_connect import create_table, insert

load_dotenv()

today = date.today()
yesterday = today - timedelta(1)

AEROAPI_KEY = os.getenv("API_KEY")

AEROAPI = requests.Session()
AEROAPI.headers.update({"x-apikey": AEROAPI_KEY})

results = AEROAPI.get(f"https://aeroapi.flightaware.com/aeroapi/airports/HKJK/flights/arrivals?start={yesterday}&end={today}&max_pages=10")

results_json = results.json()


columns = {
    "ident":"VARCHAR",
    "fa_flight_id":"VARCHAR",
    "operator":"VARCHAR",
    "flight_number":"VARCHAR",
    "inbound_fa_flight_id":"VARCHAR",
    "origin_airport_info_url":"VARCHAR",
    "destination_airport_info_url":"VARCHAR",
    "scheduled_out":"TIMESTAMP_TZ",
    "estimated_out":"TIMESTAMP_TZ",
    "actual_out":"TIMESTAMP_TZ",
    "scheduled_in":"TIMESTAMP_TZ",
    "estimated_in":"TIMESTAMP_TZ",
    "actual_in":"TIMESTAMP_TZ"
}

create_table(
    role="SYSADMIN",
    warehouse="MASTER",
    db="DEV",
    schema="FLIGHTS",
    table_name="jkia_flights",
    columns=columns
)

flights = results_json["arrivals"]
# print(flights)
counter = 0
for index, flight in enumerate(flights):
    row = [
        flight["ident"], 
        flight["fa_flight_id"],
        flight["operator"],
        flight["flight_number"],
        flight["inbound_fa_flight_id"],
        flight["origin"]["airport_info_url"],
        flight["destination"]["airport_info_url"],
        flight["scheduled_out"],
        flight["estimated_out"],
        flight["actual_out"],
        flight["scheduled_in"],
        flight["estimated_in"],
        flight["actual_in"]
    ]

        # print(row)
    insert(
        role="SYSADMIN",
        warehouse="MASTER",
        db="DEV",
        schema="FLIGHTS",
        table_name="jkia_flights",
        columns=columns,
        data=row
    )
    print(f"inserted {index + 1} of {len(flights)}")