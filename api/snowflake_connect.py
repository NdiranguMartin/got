#!/usr/bin/env python
import snowflake.connector
from dotenv import load_dotenv
import os
from jinja2 import Template

load_dotenv()


def create_table(role: str, warehouse: str, db: str, schema: str, table_name: str, columns: dict):
    conn = snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT")
    )
    cur = conn.cursor()

    try:
        cur.execute(f"USE ROLE {role}")
        cur.execute(f"USE WAREHOUSE {warehouse}")
        cur.execute(f"USE DATABASE {db}")
        cur.execute(f"USE SCHEMA {schema}")

        cur.execute(
            f"CREATE TABLE IF NOT EXISTS {table_name}"
            f'({", ".join(f"{colname} {data_type}" for colname, data_type in columns.items())})'
        )
    finally:
        cur.close()
    conn.close()
    print("table successfully created")


def insert(role: str, warehouse: str, db: str, schema: str, table_name: str, columns: dict, data):
    colnames = " ,".join(colname for colname in columns)
    data = tuple(data)
    values_placeholders = ['%s'] * len(columns)
    placeholders = ", ".join(holder for holder in values_placeholders)
    conn = snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT")
    )
    cur = conn.cursor()

    try:
        cur.execute(f"USE ROLE {role}")
        cur.execute(f"USE WAREHOUSE {warehouse}")
        cur.execute(f"USE DATABASE {db}")
        cur.execute(f"USE SCHEMA {schema}")

        cur.execute(
            Template(
                """
                INSERT INTO {{ table_name }}({{ colnames }}) VALUES
                ({{ placeholders }});
                """
            ).render(
                table_name=table_name, 
                colnames=colnames,
                placeholders=placeholders
                )
            ,data
        )
    finally:
        cur.close()
    conn.close()
    print("rows successfully inserted")
