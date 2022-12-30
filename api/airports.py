import datetime
import math
import os
import time
from datetime import date, timedelta

import requests
from alive_progress import alive_bar
from dotenv import load_dotenv
from snowflake_connect import create_table, insert, query
from src import wait

# wait_time = 1 * 60
# wait(wait_time)
AEROAPI_KEY = os.getenv("API_KEY")

AEROAPI = requests.Session()
AEROAPI.headers.update({"x-apikey": AEROAPI_KEY})


results = AEROAPI.get(
    f"https://aeroapi.flightaware.com/aeroapi/airports?max_pages=10")

results_json = results.json()
airports_list = results_json["airports"]

role = os.getenv("SNOWFLAKE_ROLE")
warehouse = os.getenv("SNOWFLAKE_WAREHOUSE")
db = os.getenv("SNOWFLAKE_DB")
schema = "FLIGHTS"
table_name = "airports"

columns = {
    "airport_code": "VARCHAR",
    "alternate_ident": "VARCHAR",
    "code_icao": "VARCHAR",
    "code_iata": "VARCHAR",
    "code_lid": "VARCHAR",
    "name": "VARCHAR",
    "type": "VARCHAR",
    "city": "VARCHAR",
    "state": "VARCHAR",
    "longitude": "FLOAT",
    "latitude": "FLOAT",
    "timezone": "VARCHAR",
    "country_code": "VARCHAR",
    "loaded_at": "BIGINT"
}


create_table(
    role=role,
    warehouse=warehouse,
    db=db,
    schema=schema,
    table_name=table_name,
    columns=columns
)

airports_query = query(
    role=role,
    warehouse=warehouse,
    db=db,
    schema=schema,
    query="SELECT airport_code FROM airports"
)

loaded_airports = [row[0] for row in airports_query]
loaded_airports = tuple(loaded_airports)

row_count = len(airports_list)

with alive_bar(row_count) as bar:
    for airport in airports_list:
        code = airport['code']
        if code not in loaded_airports:
            print(code)
            wait_time = 3 * 60
            wait(wait_time)
            airport_details = AEROAPI.get(f"https://aeroapi.flightaware.com/aeroapi/airports/{code}")
            print(airport_details)
             
            airport_details = airport_details.json()
            row = [airport_details[f"{col}"] for col in columns if col != 'loaded_at']
            row.append(math.ceil(time.time()))
            print(row)
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
    