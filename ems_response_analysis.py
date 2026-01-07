import duckdb #DuckDB will process the data with SQL
from pathlib import Path # we use Path to establish a location for the folder where our collection of monthly data files lives
from matplotlib import pyplot as plt #plotting library to graph info visually
from enum import Enum #make enumerated keywords (words with numerical values)
import pandas as pd #pandas to work with smaller result summary tables

#TEMPLATE TO FILL IN VARIABLE NAMES BY YEAR
list_of_years = ["2015", "2016", "2017", "2018", "2019", "2020", "2021", "2022", "2023", "2024", "2025"] #EDIT HERE TO UPDATE THE LIST OF YEARS TO CHECK


#CREATE DATABASE, WILL BE CLOSED AT THE END OF RUN
duck_ems_connect = duckdb.connect(database="ems.duckdb") 

#MAKE NAME FOR SQL/DUCKDB TABLE (makes port from previous analysis more convenient)
table_name = "EMS_TABLE"

#CHECK IF PARQUET HAS ALREADY BEEN CREATED, IF NOT, MAKE IT, AND THE TABLE:
parquet_path = Path("ems_response_since_2015.parquet")
csv_path = Path("ems_response_since_2015.csv")

if (not (parquet_path.exists())):

    duck_ems_connect.execute(f"""
            COPY (
                SELECT *
                FROM read_csv_auto(
                '{csv_path}'
            )
        ) TO '{parquet_path}' (FORMAT PARQUET);
    """)

    #CREATE DUCKDB TABLE
    duck_ems_connect.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} AS 
        SELECT *
        FROM read_parquet('{parquet_path}')
    """)

    #RECLASSIFY TRAVEL TIMES AS INTEGERS
    duck_ems_connect.execute(f"""
    ALTER TABLE {table_name}
    ADD COLUMN INCIDENT_TRAVEL_TM_SECONDS_QY_INT INTEGER;
    """)

    duck_ems_connect.execute(f"""
    ALTER TABLE {table_name}
    ADD COLUMN INCIDENT_RESPONSE_SECONDS_QY_INT INTEGER;
    """)

    duck_ems_connect.execute(f"""
    UPDATE {table_name}
    SET
        INCIDENT_TRAVEL_TM_SECONDS_QY_INT =
            TRY_CAST(REPLACE(INCIDENT_TRAVEL_TM_SECONDS_QY, ',', '') AS INTEGER),
        INCIDENT_RESPONSE_SECONDS_QY_INT =
            TRY_CAST(REPLACE(INCIDENT_RESPONSE_SECONDS_QY, ',', '') AS INTEGER);
    """)

#ACTUAL ANALYSIS BEGINS HERE

for year in list_of_years:
    summary_results = duck_ems_connect.execute(f"""
        SELECT INCIDENT_TRAVEL_TM_SECONDS_QY_INT
        FROM {table_name}
        LIMIT 5
    """).fetchall()

for row in summary_results:
    print(row)

#CLOSE DATABASE
duck_ems_connect.close()
