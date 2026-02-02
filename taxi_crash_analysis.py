import pandas as pd #table reading library: Pandas
from pathlib import Path # we use Path to establish a location for the folder where our collection of monthly data files lives
from matplotlib import pyplot as plt #plotting library to graph info visually
from enum import Enum #make enumerated keywords (words with numerical values)
import duckdb #for processing the sql query and applying it to persons crash sheet
import geopandas as gpd #for putting coordinate-ready person-crash data in neighborhoods w/ NTA/CRZ shapefiles
from shapely.ops import unary_union #Shapely is a geometric analysis tool. we want to be able to union different polygons together with unary_union
from shapely.geometry import Point  #                                       use Point to turn crash locations into points
import matplotlib.dates as mdates #use mdates to set year intervals manually
import json #to read and parse geojson files of certain areas
import re #regex for char/pattern recognition

#boro names matched to boro codes
boroughs = {
    "1": "Manhattan",
    "2": "Bronx",
    "3": "Brooklyn",
    "4": "Queens",
    "5": "Staten Island"
}

#number of community boards per borough
district_counts = {
    "1": 12,   # Manhattan CBs 1–12
    "2": 12,   # Bronx CBs 1–12
    "3": 18,   # Brooklyn CBs 1–18
    "4": 14,   # Queens CBs 1–14
    "5": 3     # Staten Island CBs 1–3
}

#CREATE DATABASE, WILL BE CLOSED AT THE END OF RUN
duck_taxi_connect = duckdb.connect(database="crashes.duckdb") 

#ENABLE SPATIAL ELEMENT DUCKDB
duck_taxi_connect.execute("INSTALL spatial;")
duck_taxi_connect.execute("LOAD spatial;")

#MAKE NAME FOR SQL/DUCKDB TABLE (makes port from previous analyses more convenient)
table_name = "CRASH_TABLE"

#CHECK IF PARQUET HAS ALREADY BEEN CREATED, IF NOT, MAKE IT, AND THE TABLE:
parquet_path = Path("crashes.parquet")
csv_path = Path("crashes.csv")

if (not (parquet_path.exists())):

    #CONVERT CSV TO PARQUET
    duck_taxi_connect.execute(f"""
        COPY (
            FROM read_csv(
                '{csv_path}',
                all_varchar = true
            )
        ) TO '{parquet_path}' (FORMAT PARQUET);
    """)

    #CREATE DUCKDB TABLE
    duck_taxi_connect.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} AS 
        SELECT *
        FROM read_parquet('{parquet_path}')
    """)

    #ADD GEOMETRY COLUMN FOR CRASH POINT, FLIP LAT AND LONG IN LOCATION IN SHEET TO MATCH WITH WKT FORMAT
    duck_taxi_connect.execute(f"""
        ALTER TABLE {table_name}
        ADD COLUMN IF NOT EXISTS crash_geom GEOMETRY;

        UPDATE {table_name}
        SET crash_geom = ST_Point(
            CAST(SPLIT(REPLACE(REPLACE(LOCATION, '(', ''), ')', ''), ',')[2] AS DOUBLE),
            CAST(SPLIT(REPLACE(REPLACE(LOCATION, '(', ''), ')', ''), ',')[1] AS DOUBLE)  
        );    
""")

        #make table "regions" to store every predefined polygon we use
    duck_taxi_connect.execute(f"""

        CREATE TABLE IF NOT EXISTS regions (
            region_name VARCHAR,
            geom GEOMETRY
        );

    """)

    #IMPORT CBS JSON AND CONVERT CONTENT TO WKT BOROCD - COORDINATE STRING FOR REGION TABLE CREATION
    with open("nyc_cbs.json", "r") as f:
        data = json.load(f)

        rows = data["data"]

        cb_output_to_string = ""

        for row in rows:
            # columns based on the file structure
            boro_cd = row[8]         # Boro+Cd
            wkt = row[11]            # the_geom (already WKT)

            cb_output_to_string+=(f"""
        (
        'cb_{boro_cd}',
        ST_GeomFromText('{wkt}')
        ),
        """)

        cb_output_to_string = cb_output_to_string.rstrip(",\n")
        cb_output_to_string += ";"

    #insert polygons into regions
    duck_taxi_connect.execute(f"""
        INSERT INTO regions (region_name, geom)
        VALUES
        {cb_output_to_string}
    """)

    #change table to add community_board as a column
    duck_taxi_connect.execute(f"""
        ALTER TABLE {table_name}
        ADD COLUMN community_board VARCHAR;
    """)

    #join geometry of crash w CBs geometry
    duck_taxi_connect.execute(f"""
        UPDATE {table_name} c
        SET community_board = r.region_name
        FROM regions r
        WHERE ST_Intersects(c.crash_geom, r.geom);
    """)


#ACTUAL ANALYSIS BEGINS HERE

first_30_test = duck_taxi_connect.execute(f"""
    SELECT *
    FROM {table_name}
    LIMIT 30;
""").fetchall()


taxi_crash_yearly = duck_taxi_connect.execute(f"""
    SELECT
        EXTRACT(YEAR FROM STRPTIME("CRASH DATE", '%m/%d/%Y')) AS crash_year,
        COUNT(*) AS taxi_count,
        COUNT(CASE 
                WHEN ("community_board" IN ('cb_101','cb_102','cb_103','cb_104','cb_105','cb_106')) 
                THEN 1 
            END) AS taxi_count_cbd,
        COUNT(CASE 
                WHEN ("community_board" NOT IN ('cb_101','cb_102','cb_103','cb_104','cb_105','cb_106')) 
                THEN 1 
            END) AS taxi_count_other
    FROM {table_name}
    WHERE (
        "VEHICLE TYPE CODE 1" = 'Taxi' OR
        "VEHICLE TYPE CODE 2" = 'Taxi' OR
        "VEHICLE TYPE CODE 3" = 'Taxi' OR
        "VEHICLE TYPE CODE 4" = 'Taxi' OR
        "VEHICLE TYPE CODE 5" = 'Taxi'
        )
    AND (
    CAST("NUMBER OF PERSONS INJURED" AS INTEGER) > 0 OR CAST("NUMBER OF PERSONS KILLED" AS INTEGER) > 0
    )
    GROUP BY crash_year
    ORDER BY crash_year;
""").fetchall()



print(taxi_crash_yearly)

#CLOSE DATABASE
duck_taxi_connect.close()