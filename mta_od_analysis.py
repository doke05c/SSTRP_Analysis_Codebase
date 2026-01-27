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


#ACTUAL ANALYSIS BEGINS HERE

#make table "regions" to store every predefined polygon we use
duck_od_connect.execute(f"""

    CREATE TABLE IF NOT EXISTS regions (
        region_name VARCHAR,
        geom GEOMETRY
    );

""")

#insert polygons into regions
duck_od_connect.execute("DELETE FROM regions;")
duck_od_connect.execute("""
    INSERT INTO regions (region_name, geom)
    VALUES
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

#get all the stations in the "west side" polygon
# list_of_west_side_stations = duck_od_connect.execute(f"""
# SELECT DISTINCT
#     od."Origin Station Complex Name" AS origin_station_name
# FROM {table_name} od
# JOIN regions r
#   ON r.region_name = 'west_side_58_to_149'
#  AND ST_Within(od.origin_geom, r.geom)
# ORDER BY origin_station_name;
# """).fetchall()

# #get all the stations in the "east side" polygon
# list_of_east_side_stations = duck_od_connect.execute(f"""
# SELECT DISTINCT
#     od."Origin Station Complex Name" AS origin_station_name
# FROM {table_name} od
# JOIN regions r
#   ON r.region_name = 'east_side_58_to_131'
#  AND ST_Within(od.origin_geom, r.geom)
# ORDER BY origin_station_name;
# """).fetchall()

# print("List of Stations in the Upper West Side up to 145th St:", list_of_west_side_stations)
# print("List of Stations in the Upper East Side up to 125th St:", list_of_east_side_stations)

# tot_sum, southbound_125, northbound_125 = duck_od_connect.execute(f"""
#     SELECT

#         COUNT(*) AS tot_sum,

#         SUM(
#             CASE
#                 WHEN
#                     "Origin Station Complex Name" = '125 St (1)'
#                     AND "Destination Station Complex Name" = '34 St-Penn Station (1,2,3)'
#                 THEN "Estimated Average Ridership"
#                 ELSE 0
#             END
#         ) AS southbound_125,

#         SUM(
#             CASE
#                 WHEN
#                     "Origin Station Complex Name" = '34 St-Penn Station (1,2,3)'
#                     AND "Destination Station Complex Name" = '125 St (1)'
#                 THEN "Estimated Average Ridership"
#                 ELSE 0
#             END
#         ) AS northbound_125

#     FROM {table_name}
# """).fetchone()

west_to_east, east_to_west, west_harlem_to_east_harlem, east_harlem_to_west_harlem, upper_west_to_upper_east, upper_east_to_upper_west = duck_od_connect.execute(f"""

SELECT
    SUM(
        CASE
            WHEN ST_Within(origin_geom, (SELECT geom FROM regions WHERE region_name = 'west_side_58_to_149'))
            AND ST_Within(destination_geom, (SELECT geom FROM regions WHERE region_name = 'east_side_58_to_131'))
            THEN "Estimated Average Ridership"
            ELSE 0
        END
    ) AS west_to_east,

    SUM(
        CASE
            WHEN ST_Within(origin_geom, (SELECT geom FROM regions WHERE region_name = 'east_side_58_to_131'))
            AND ST_Within(destination_geom, (SELECT geom FROM regions WHERE region_name = 'west_side_58_to_149'))
            THEN "Estimated Average Ridership"
            ELSE 0
        END
    ) AS east_to_west,

    SUM(
        CASE
            WHEN ST_Within(origin_geom, (SELECT geom FROM regions WHERE region_name = 'west_harlem_125_to_149'))
            AND ST_Within(destination_geom, (SELECT geom FROM regions WHERE region_name = 'east_harlem_96_to_131'))
            THEN "Estimated Average Ridership"
            ELSE 0
        END
    ) AS west_harlem_to_east_harlem,

    SUM(
        CASE
            WHEN ST_Within(origin_geom, (SELECT geom FROM regions WHERE region_name = 'east_harlem_96_to_131'))
            AND ST_Within(destination_geom, (SELECT geom FROM regions WHERE region_name = 'west_harlem_125_to_149'))
            THEN "Estimated Average Ridership"
            ELSE 0
        END
    ) AS east_harlem_to_west_harlem,

    SUM(
        CASE
            WHEN ST_Within(origin_geom, (SELECT geom FROM regions WHERE region_name = 'uws_58_to_125'))
            AND ST_Within(destination_geom, (SELECT geom FROM regions WHERE region_name = 'ues_58_to_96'))
            THEN "Estimated Average Ridership"
            ELSE 0
        END
    ) AS upper_west_to_upper_east,

    SUM(
        CASE
            WHEN ST_Within(origin_geom, (SELECT geom FROM regions WHERE region_name = 'ues_58_to_96'))
            AND ST_Within(destination_geom, (SELECT geom FROM regions WHERE region_name = 'uws_58_to_125'))
            THEN "Estimated Average Ridership"
            ELSE 0
        END
    ) AS upper_east_to_upper_west

FROM {table_name};
""").fetchone()

# print("Number of rows taken in:", tot_sum)
print("Apprx Number of Eastbound Subway Trips in 2024 Between Upper West (59-145) and Upper East (59-125):", west_to_east * 4.348)
print("Apprx Number of Westbound Subway Trips in 2024 Between Upper East (59-125) and Upper West (59-145):", east_to_west * 4.348)
print("Apprx Number of Eastbound Subway Trips in 2024 Between West Harlem (125-145) and East Harlem (96-131):", west_harlem_to_east_harlem * 4.348)
print("Apprx Number of Westbound Subway Trips in 2024 Between East Harlem (96-131) and West Harlem (125-145):", east_harlem_to_west_harlem * 4.348)
print("Apprx Number of Eastbound Subway Trips in 2024 Between Upper West Side (59-125) and Upper East Side (59-96):", upper_west_to_upper_east * 4.348)
print("Apprx Number of Westbound Subway Trips in 2024 Between Upper East Side (59-96) and Upper West Side (59-125):", upper_east_to_upper_west * 4.348)


#CLOSE DATABASE
duck_od_connect.close()