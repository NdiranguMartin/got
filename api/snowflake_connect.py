#!/usr/bin/env python
import snowflake.connector
from dotenv import load_dotenv
import os
from jinja2 import Template

load_dotenv()

def con():
    conn = snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT")  
    )
    
    return conn

def create_table(role: str, warehouse: str, db: str, schema: str, table_name: str, columns: dict, drop_if_exist = False):
    """
    This functions creates a table from the arguments supplied. 
    """
    conn = con()
    cur = conn.cursor()

    try:
        cur.execute(f"USE ROLE {role}")
        cur.execute(f"USE WAREHOUSE {warehouse}")
        cur.execute(f"USE DATABASE {db}")
        cur.execute(f"USE SCHEMA {schema}")
        if drop_if_exist:
            cur.execute(f'DROP TABLE {table_name} CASCADE')
            print('Table dropped if it existed')
        cur.execute(
            f"CREATE TABLE IF NOT EXISTS {table_name}"
            f'({", ".join(f"{colname} {data_type}" for colname, data_type in columns.items())})'
        )
    finally:
        cur.close()
    conn.close()
    print("table successfully created, if it did not exist")


def insert(role: str, warehouse: str, db: str, schema: str, table_name: str, columns: dict, data):
    colnames = " ,".join(colname for colname in columns)
    data = tuple(data)
    values_placeholders = ['%s'] * len(columns)
    placeholders = ", ".join(holder for holder in values_placeholders)
    
    conn = con()
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


def query(role: str, warehouse: str, db: str, schema: str, query: str):
    """
    This functions creates a table from the arguments supplied. 
    """
    conn = con()
    cur = conn.cursor()

    try:
        cur.execute(f"USE ROLE {role}")
        cur.execute(f"USE WAREHOUSE {warehouse}")
        cur.execute(f"USE DATABASE {db}")
        cur.execute(f"USE SCHEMA {schema}")
        cur.execute(query)
        results = cur.fetchall()
    finally:
        cur.close()
    conn.close()
    print("Query executed")
    return results
