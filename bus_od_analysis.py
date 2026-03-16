import duckdb #DuckDB will process the data with SQL
from pathlib import Path # we use Path to establish a location for the folder where our collection of monthly data files lives
from matplotlib import pyplot as plt #plotting library to graph info visually
from enum import Enum #make enumerated keywords (words with numerical values)
import pandas as pd #pandas to work with smaller result summary tables
import matplotlib.dates as mdates #use mdates to set year intervals manually
import json #to read and parse geojson files of certain areas
import re #regex for char/pattern recognition

##PLAN:

#STAGE 1: Join lat/long to a particular CB
#STAGE 2: Make a boarding matrix and a deboarding matrix for all stops on each CB (how many boards/deboards for CB x to CB y for all (x,y)?)
#STAGE 3: Make line features between the n and n-1th point starting from 1st-2nd up to n-1th-nth
#STAGE 4: Port to ArcGIS and make visual on how many people board/deboard/total on bus line (filter by direction?)

bus_ridership_df = pd.read_csv("oct_2025_bus_ridership.csv")

#initialize DuckDB database for the bus analysis
duck_bus_ridership_connect = duckdb.connect(database="bus_ridership.duckdb") 

#use spatial element
duck_bus_ridership_connect.execute("INSTALL spatial;")
duck_bus_ridership_connect.execute("LOAD spatial;")

duck_bus_ridership_connect.register("bus_ridership_df_view", bus_ridership_df)

duck_bus_ridership_connect.execute(f"""
    CREATE OR REPLACE TABLE bus_ridership (
        Route_ID TEXT,
        Stop_ID TEXT,
        Stop_Name TEXT,
        Route_Stop_Order INTEGER,
        Weekday_Ons INTEGER,
        Weekday_Offs INTEGER,
        Weekday_Leave_Load INTEGER,
        Saturday_Ons INTEGER,
        Saturday_Offs INTEGER,
        Saturday_Leave_Load INTEGER,
        Sunday_Ons INTEGER,
        Sunday_Offs INTEGER,
        Sunday_Leave_Load INTEGER,
        Direction TEXT,
        Borough TEXT,
        Latitude DOUBLE,
        Longitude DOUBLE
    );
""")

duck_bus_ridership_connect.execute(f"""
    INSERT INTO bus_ridership
    SELECT *
    FROM bus_ridership_df_view
""")

duck_bus_ridership_connect.execute("""
    ALTER TABLE bus_ridership
    ADD COLUMN IF NOT EXISTS georeference GEOMETRY;
""")

duck_bus_ridership_connect.execute("""
    UPDATE bus_ridership
    SET georeference = ST_Point(Longitude, Latitude);
""")


duck_bus_ridership_connect.execute(f"""
    ALTER TABLE bus_ridership
    ADD COLUMN IF NOT EXISTS community_board TEXT;
""")

#make table "regions" to store every predefined polygon we use
duck_bus_ridership_connect.execute(f"""

    CREATE OR REPLACE TABLE regions (
        region_name TEXT,
        geom GEOMETRY
    );

""")

#IMPORT CBS JSON AND CONVERT CONTENT TO WKT BOROCD - COORDINATE STRING FOR REGION TABLE CREATION
with open("../nyc_cbs.json", "r") as f:
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
duck_bus_ridership_connect.execute(f"""
    INSERT INTO regions (region_name, geom)
    VALUES
    {cb_output_to_string}
""")

#remove the CBs that are parks, these eat up stations near central park, prospect park, etc.
duck_bus_ridership_connect.execute(f"""
    CREATE OR REPLACE TABLE regions_no_parks AS (
        SELECT *
        FROM regions
        WHERE region_name NOT IN ('cb_164', 'cb_226', 'cb_355', 'cb_481', 'cb_483')
    );
""")


duck_bus_ridership_connect.execute(f"""
    CREATE OR REPLACE TABLE cb_nearest AS
        SELECT
            buses.georeference AS bus_geom,
            r.region_name AS cb_region
        FROM (SELECT DISTINCT georeference FROM bus_ridership) buses
        CROSS JOIN regions_no_parks r
    QUALIFY
        ST_Distance(buses.georeference, r.geom)
        = MIN(ST_Distance(buses.georeference, r.geom))
        OVER (PARTITION BY buses.georeference);
""")

duck_bus_ridership_connect.execute(f"""
    UPDATE bus_ridership AS b
    SET community_board = r.cb_region
    FROM cb_nearest AS r
    WHERE b.georeference = r.bus_geom;
""")

duck_bus_ridership_connect.execute(f"""
    CREATE OR REPLACE TABLE cb_patterns AS
    SELECT
        community_board,
        SUM(Weekday_Ons) AS weekday_ons_cb,
        SUM(Weekday_Offs) AS weekday_offs_cb,
        SUM(Saturday_Ons) AS saturday_ons_cb,
        SUM(Saturday_Offs) AS saturday_offs_cb,
        SUM(Sunday_Ons) AS sunday_ons_cb,
        SUM(Sunday_Offs) AS sunday_offs_cb
    FROM bus_ridership
    GROUP BY community_board;
""")

#EXPORT TO CSV
duck_bus_ridership_connect.execute("""
    COPY (
        SELECT *
        FROM cb_patterns
    )
    TO 'cb_patterns_test.csv'
    (FORMAT CSV, HEADER);
""")

# regions_preview = duck_bus_ridership_connect.execute(f"""
#     SELECT * FROM regions
# """).fetchall()

# print(regions_preview)

bus_ridership_preview = duck_bus_ridership_connect.execute(f"""
    SELECT * FROM bus_ridership
    ORDER BY Route_ID, Direction, Route_Stop_Order ASC
    LIMIT 2
""").fetchall()

print(bus_ridership_preview)