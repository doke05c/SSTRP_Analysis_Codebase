import duckdb #DuckDB will process the data with SQL
from pathlib import Path # we use Path to establish a location for the folder where our collection of monthly data files lives
from matplotlib import pyplot as plt #plotting library to graph info visually
from enum import Enum #make enumerated keywords (words with numerical values)
import pandas as pd #pandas to work with smaller result summary tables
import matplotlib.dates as mdates #use mdates to set year intervals manually

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

    #insert polygons into regions
    duck_od_connect.execute("""
        INSERT INTO regions (region_name, geom)
        VALUES
        (
            'midtown_21_to_57',
            ST_GeomFromText(
                'POLYGON ((
                    -73.9917397 40.7709869,
                    -74.0090561 40.7482486,
                    -73.9724278 40.7329174,
                    -73.9570427 40.7560998,
                    -73.9917397 40.7709869
                ))'
            )
        ),

        (
            'uws_58_to_125',
            ST_GeomFromText(
                'POLYGON ((
                    -73.9682007 40.8131596,
                    -73.9962244 40.7735056,
                    -73.9810538 40.7671354,
                    -73.9571500 40.8009622,
                    -73.9609480 40.8026514,
                    -73.9573431 40.8091807,
                    -73.9682007 40.8131596
                ))'
            )
        ),

        (
            'ues_58_to_96',
            ST_GeomFromText(
                'POLYGON ((
                    -73.9557552 40.7898997,
                    -73.9742088 40.7638363,
                    -73.9578152 40.7574326,
                    -73.9576435 40.7574326,
                    -73.9399624 40.7832386,
                    -73.9557552 40.7898997
                ))'
            )
        ),

        (
            'west_harlem_125_to_149',
            ST_GeomFromText(
                'POLYGON ((
                    -73.9542317 40.8310863,
                    -73.9665270 40.8153358,
                    -73.9584696 40.8122745,
                    -73.9506269 40.8090914,
                    -73.9394689 40.8251438,
                    -73.9542317 40.8310863
                ))'
            )
        ),
        (
            'east_harlem_96_to_131',
            ST_GeomFromText(
                'POLYGON (( 
                -73.9397478 40.8104150, 
                -73.9557552 40.7898997, 
                -73.9378810 40.782104, 
                -73.9200497 40.8024403, 
                -73.9397478 40.8104150 
                ))'
            )
        ),

        (
            'west_side_58_to_149',
            ST_GeomFromText(
                'POLYGON ((
                    -73.9563346 40.8323689,
                    -73.9982629 40.7740094,
                    -73.9812684 40.7668754,
                    -73.9394474 40.82516,
                    -73.9563346 40.8323689
                ))'
            )
        ),

        (
            'east_side_58_to_131',
            ST_GeomFromText(
                'POLYGON ((
                    -73.9399409 40.8103663,
                    -73.9740372 40.7638525,
                    -73.9564848 40.7566687,
                    -73.9224315 40.8034636,
                    -73.9399409 40.8103663
                ))'
            )
        );
    """)

#define a flow matrix from r1 (all regions) to r2 (same list of all regions, aka all possibilites), (also includes "within" category), taken from od table
#FOR NOW: multiplies by 4.348 to get an annual ridership estimate (# of weeks in a month)

# ONLY COMPUTE FLOWS IF THEY DONT EXIST YET
flow_path = Path("flow_matrix.parquet")
if not flow_path.exists():

    flow_matrix = duck_od_connect.execute(f"""
        WITH od_labeled AS (
            SELECT
                r1.region_name AS origin_region,
                r2.region_name AS destination_region,
                od."Estimated Average Ridership" AS ridership
            FROM {table_name} od
            JOIN regions r1
            ON ST_Within(od.origin_geom, r1.geom)
            JOIN regions r2
            ON ST_Within(od.destination_geom, r2.geom)
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

print(flow_dict)


#LIST QUANTITIES OF RELATIONAL TRAVEL PATTERNS BETWEEN POLYGONAL REGIONS

#DICTIONARY TO TURN INTERNAL NAMES TO NICER NAMES FOR PRINTING
region_labels = {
    'west_side_58_to_149': 'West Side (58-145)',
    'east_side_58_to_131': 'East Side (58-125)',
    'midtown_21_to_57': 'Midtown (21-57)',
    'uws_58_to_125': 'Upper West Side (58-125)',
    'ues_58_to_96': 'Upper East Side (58-96)',
    'west_harlem_125_to_149': 'West Harlem (125-145)',
    'east_harlem_96_to_131': 'East Harlem (97-125)'
}

#LOOP THROUGH FLOW_DICT AND REGION_LABELS TO PRINT OUT NUMBERS
for (origin, destination), trips in flow_dict.items():

    #ORIGIN NAME IS PRETTIFIED VERSION OF ORIGIN INTERNAL NAME, ELSE: DEFAULT TO INTERNAL ORIGIN NAME
    origin_name = region_labels.get(origin, origin)

    #ORIGIN NAME IS PRETTIFIED VERSION OF DESTINATION INTERNAL NAME, ELSE: DEFAULT TO INTERNAL DESTINATION NAME
    destination_name = region_labels.get(destination, destination)

    print(f"{origin_name} to {destination_name} Estimated Trip Count in 2024: {int(trips):,} trips")


#CLOSE DATABASE
duck_od_connect.close()