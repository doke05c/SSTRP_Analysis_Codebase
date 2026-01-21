import duckdb #DuckDB will process the data with SQL
from pathlib import Path # we use Path to establish a location for the folder where our collection of monthly data files lives
from matplotlib import pyplot as plt #plotting library to graph info visually
from enum import Enum #make enumerated keywords (words with numerical values)
import pandas as pd #pandas to work with smaller result summary tables
import matplotlib.dates as mdates #use mdates to set year intervals manually

#CREATE DATABASE, WILL BE CLOSED AT THE END OF RUN
duck_od_connect = duckdb.connect(database="od.duckdb") 

#MAKE NAME FOR SQL/DUCKDB TABLE (makes port from previous analyses more convenient)
table_name = "OD_TABLE"

#CHECK IF PARQUET HAS ALREADY BEEN CREATED, IF NOT, MAKE IT, AND THE TABLE:
parquet_path = Path("mta_od_estimate_2024.parquet")
csv_path = Path("mta_od_estimate_2024.csv")

if (not (parquet_path.exists())):

    #CONVERT CSV TO PARQUET, FIX ISSUE WHERE ESTIMATED AVERAGE RIDERSHIP IS LISTED WITH THOUSANDS COMMA, REMOVE IT AND CAST APPROPRIATELY
    duck_od_connect.execute(f"""
        COPY (
            SELECT
                * EXCLUDE ("Estimated Average Ridership"),
                CAST(
                    REPLACE("Estimated Average Ridership", ',', '')
                    AS DOUBLE
                ) AS "Estimated Average Ridership"
            FROM read_csv(
                '{csv_path}',
                all_varchar = true
            )
        ) TO '{parquet_path}' (FORMAT PARQUET);
    """)

    #CREATE DUCKDB TABLE
    duck_od_connect.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} AS 
        SELECT *
        FROM read_parquet('{parquet_path}')
    """)

#ACTUAL ANALYSIS BEGINS HERE


#CLOSE DATABASE
duck_od_connect.close()