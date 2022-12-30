import datetime
import math
import os
import time
from datetime import date, timedelta

import requests
from alive_progress import alive_bar
from dotenv import load_dotenv
from snowflake_connect import create_table, insert, query
from src import to_unix

load_dotenv()

today = date.today()
yesterday = today - timedelta(1)

AEROAPI_KEY = os.getenv("API_KEY")

AEROAPI = requests.Session()
AEROAPI.headers.update({"x-apikey": AEROAPI_KEY})

results = AEROAPI.get(
    f"https://aeroapi.flightaware.com/aeroapi/airports/HKJK/flights/arrivals?start={yesterday}&end={today}&max_pages=10")

results_json = results.json()

role = os.getenv("SNOWFLAKE_ROLE")
warehouse = os.getenv("SNOWFLAKE_WAREHOUSE")
db = os.getenv("SNOWFLAKE_DB")
schema = "FLIGHTS"
table_name = "jkia_flights"

columns = {
    "ident": "VARCHAR",
    "fa_flight_id": "VARCHAR",
    "operator": "VARCHAR",
    "flight_number": "VARCHAR",
    "inbound_fa_flight_id": "VARCHAR",
    "origin_airport_info_url": "VARCHAR",
    "destination_airport_info_url": "VARCHAR",
    "scheduled_out": "BIGINT",
    "estimated_out": "BIGINT",
    "actual_out": "BIGINT",
    "scheduled_in": "BIGINT",
    "estimated_in": "BIGINT",
    "actual_in": "BIGINT",
    "loaded_at" : "BIGINT"
}

create_table(
    role=role,
    warehouse=warehouse,
    db=db,
    schema=schema,
    table_name=table_name,
    columns=columns,
)

flights = results_json["arrivals"]
# print(flights)
counter = 0
row_count = len(flights)
with alive_bar(row_count) as bar:
    for index, flight in enumerate(flights):
        row = [
            flight["ident"],
            flight["fa_flight_id"],
            flight["operator"],
            flight["flight_number"],
            flight["inbound_fa_flight_id"],
            flight["origin"]["airport_info_url"],
            flight["destination"]["airport_info_url"],
            to_unix(flight["scheduled_out"]),
            to_unix(flight["estimated_out"]),
            to_unix(flight["actual_out"]),
            to_unix(flight["scheduled_in"]),
            to_unix(flight["estimated_in"]),
            to_unix(flight["actual_in"]),
            math.ceil(time.time())
        ]
        insert(
            role=role,
            warehouse=warehouse,
            db=db,
            schema=schema,
            table_name=table_name,
            columns=columns,
            data=row
        )
        time.sleep(0.03)
        bar()