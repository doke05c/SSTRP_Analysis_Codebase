import duckdb #DuckDB will process the data with SQL
from pathlib import Path # we use Path to establish a location for the folder where our collection of monthly data files lives
from matplotlib import pyplot as plt #plotting library to graph info visually
from enum import Enum #make enumerated keywords (words with numerical values)
import pandas as pd #pandas to work with smaller result summary tables
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
duck_od_connect = duckdb.connect(database="od.duckdb") 

#ENABLE SPATIAL ELEMENT DUCKDB
duck_od_connect.execute("INSTALL spatial;")
duck_od_connect.execute("LOAD spatial;")

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

    #ADD GEOMETRY COLUMN FOR ORIGIN POINT
    duck_od_connect.execute(f"""
        ALTER TABLE {table_name}
        ADD COLUMN origin_geom GEOMETRY;

        UPDATE {table_name}
        SET origin_geom = ST_GeomFromText("Origin Point");
    """)

    #ADD GEOMETRY COLUMN FOR DESTINATION POINT
    duck_od_connect.execute(f"""
        ALTER TABLE {table_name}
        ADD COLUMN destination_geom GEOMETRY;

        UPDATE {table_name}
        SET destination_geom = ST_GeomFromText("Destination Point");
    """)

    #make table "regions" to store every predefined polygon we use
    duck_od_connect.execute(f"""

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
    duck_od_connect.execute(f"""
        INSERT INTO regions (region_name, geom)
        VALUES
        {cb_output_to_string}
    """)

#define a flow matrix from r1 (all regions) to r2 (same list of all regions, aka all possibilites), (also includes "within" category), taken from od table
#FOR NOW: multiplies by 4.348 to get an annual ridership estimate (# of weeks in a month)

# ONLY COMPUTE FLOWS IF THEY DONT EXIST YET
flow_path = Path("flow_matrix.parquet")
if not flow_path.exists():
    
    #remove the CBs that are parks, these eat up stations near central park, prospect park, etc.

    duck_od_connect.execute(f"""
        CREATE TABLE IF NOT EXISTS regions_no_parks AS (
            SELECT *
            FROM regions
            WHERE region_name NOT IN ('cb_164', 'cb_355', 'cb_481', 'cb_483')
        );
    """)

    #after that, calculate the nearest cb to each station (if station ends up outside the official cb boundary)

    #ORIGIN
    duck_od_connect.execute(f"""
        CREATE TABLE origin_nearest AS
        SELECT
            od.origin_geom,
            r.region_name AS origin_region
        FROM (SELECT DISTINCT origin_geom FROM {table_name}) od
        CROSS JOIN regions_no_parks r
        QUALIFY ST_Distance(od.origin_geom, r.geom) = MIN(ST_Distance(od.origin_geom, r.geom)) OVER (PARTITION BY od.origin_geom);
    """)

    #DESTINATION
    duck_od_connect.execute(f"""
        CREATE TABLE destination_nearest AS
        SELECT
            od.destination_geom,
            r.region_name AS destination_region
        FROM (SELECT DISTINCT destination_geom FROM {table_name}) od
        CROSS JOIN regions_no_parks r
        QUALIFY ST_Distance(od.destination_geom, r.geom) = MIN(ST_Distance(od.destination_geom, r.geom)) OVER (PARTITION BY od.destination_geom);
    """)

    #EXPORT TO CSV
    duck_od_connect.execute("""
        COPY (
            SELECT *
            FROM destination_nearest
        )
        TO 'test_sta_geom.csv'
        (FORMAT CSV, HEADER);
    """)


    #THEN calculate flow matrix
    #ST_DWithin guarantees location matching within 0.000001 of a degree of coordinate, allows leeway by a few inches in case of rounding issues.
    flow_matrix = duck_od_connect.execute(f"""
        WITH od_labeled AS (
            SELECT
                o.origin_region,
                d.destination_region,
                od."Estimated Average Ridership" AS ridership
            FROM {table_name} od
            JOIN origin_nearest o
            ON ST_DWithin(od.origin_geom, o.origin_geom, 1e-6)
            JOIN destination_nearest d
            ON od.destination_geom = d.destination_geom
        )

        SELECT
            origin_region,
            destination_region,
            SUM(ridership) * 4.348 AS annual_ridership_estimate
        FROM od_labeled
        GROUP BY origin_region, destination_region
        ORDER BY origin_region, destination_region;
    """).fetchall()

    #SAVE
    import pandas as pd
    pd.DataFrame(flow_matrix, columns=['origin_region', 'destination_region', 'annual_ridership_estimate']).to_parquet(flow_path)

    #CONVERT TO DICT, KEY IS (ORIGIN, DESTINATION), WHILE FLOW_MATRIX IS STILL A VARIABLE
    flow_dict = {
        (origin, dest): value
        for origin, dest, value in flow_matrix
    }

else:
    #IF EXISTS, JUST READ FROM DISK
    import pandas as pd
    flow_matrix = pd.read_parquet(flow_path)
    
    #CONVERT TO DICT, KEY IS (ORIGIN, DESTINATION), AFTER WE RUN PROGRAM WITHOUT LONG PROCESSING PHASE
    flow_dict = {
        (origin, dest): value
        for origin, dest, value in flow_matrix.itertuples(index=False, name=None)
    }


#ACTUAL ANALYSIS BEGINS HERE

# print(flow_dict)


#LIST QUANTITIES OF RELATIONAL TRAVEL PATTERNS BETWEEN POLYGONAL REGIONS

#DICTIONARY TO TURN INTERNAL NAMES TO NICER NAMES FOR PRINTING
region_labels = {}

#FILL REGION_LABELS WITH NAMES
for boro_code, boro_name in boroughs.items():
    for cd in range(1, district_counts[boro_code] + 1):
        cd_str = f"{cd:02d}"   # zero-pad (01, 02, ...)
        key = f"cb_{boro_code}{cd_str}"
        value = f"{boro_name} Community Board {cd}"

        region_labels[key] = value

#RENAMINGS BASED ON OBSERVED GEOMETRIES
region_labels["cb_164"] = "Central Park"
region_labels["cb_355"] = "Prospect Park"
region_labels["cb_481"] = "Flushing Meadows-Corona Park"
region_labels["cb_483"] = "JFK Airport"


#LOOP THROUGH FLOW_DICT AND REGION_LABELS TO PRINT OUT NUMBERS
for (origin, destination), trips in flow_dict.items():

    #ORIGIN NAME IS PRETTIFIED VERSION OF ORIGIN INTERNAL NAME, ELSE: DEFAULT TO INTERNAL ORIGIN NAME
    origin_name = region_labels.get(origin, origin)

    #ORIGIN NAME IS PRETTIFIED VERSION OF DESTINATION INTERNAL NAME, ELSE: DEFAULT TO INTERNAL DESTINATION NAME
    destination_name = region_labels.get(destination, destination)

    if (origin_name == "Manhattan Community Board 7") and ("Manhattan" in destination_name):
        print(f"{origin_name} to {destination_name} Estimated Trip Count in 2024: {int(trips):,} trips")

    

#TABLE-IZE DATA

def sort_key(name):
    area_match = re.match(r"(\w+) Community Board (\d+)", name)
    if area_match:
        borough, num = area_match.groups()
        return (borough, int(num))
    else:
        return (name, 0) #for parks/airport

#CONVERT FLOW DICT TO DATAFRAME
df_long = pd.DataFrame(
    [(region_labels.get(origin, origin), region_labels.get(destination, destination), value) for (origin, destination), value in flow_dict.items()],
    columns=["origin", "destination", "annual_ridership"]
)

#PIVOT TO 2D
df_matrix = df_long.pivot(index="origin", columns="destination", values="annual_ridership")

df_matrix = df_matrix.reindex(sorted(df_matrix.index, key=sort_key))
df_matrix = df_matrix.reindex(sorted(df_matrix.columns, key=sort_key), axis=1)


# EXPORT TO CSV
# df_matrix.to_csv("2024_flow_matrix_2d.csv")

#CLOSE DATABASE
duck_od_connect.close()