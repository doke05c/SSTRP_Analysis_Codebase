import duckdb #DuckDB will process the data with SQL
from pathlib import Path # we use Path to establish a location for the folder where our collection of monthly data files lives
from matplotlib import pyplot as plt #plotting library to graph info visually
from enum import Enum #make enumerated keywords (words with numerical values)

column_dict = {
    "ride_id": "column00",
    "rideable_type": "column01",
    "start yyyy=mm-dd hh" : "column02",
    "start mm" : "column03",
    "start ss" : "column04",
    "end yyyy-mm-dd hh" : "column05",
    "end mm" : "column06",
    "end ss" : "column07",
    "start_name" : "column08",
    "start_ID" : "column09",
    "end_name" : "column10",
    "end_ID" : "column11",
    "start_lat" : "column12",
    "start_long" : "column13",
    "end_lat" : "column14",
    "end_long" : "column15",
    "rider_type" : "column16",
}   


#TEMPLATE TO FILL IN VARIABLE NAMES BY YEAR
list_of_years = ["2024", "2025"] #EDIT HERE TO UPDATE THE LIST OF YEARS TO CHECK
list_of_source_files_csv = {} #source file dictionary, keeps track of source files through the years
list_of_source_files_parquet = {} #source file dictionary, keeps track of source files through the years
list_of_tables = {} #table dictionary, keeps track of tables through the years

for year in list_of_years: #iterate through the years to create the variable names (excl. parquet)

    list_of_source_files_csv[year] = Path(f"{year}-citibike-tripdata-full.csv") #ex: 2024-citibike-tripdata-full.csv 

    list_of_tables[year] = f"citibike_{year}"



#CREATE DATABASE, WILL BE CLOSED AT THE END OF RUN
duck_citibike_connect = duckdb.connect(database="citibike.duckdb") 

#INSTALL/LOAD SPATIAL CONNECTION
duck_citibike_connect.execute("INSTALL spatial;")
duck_citibike_connect.execute("LOAD spatial;")


#CHECK IF PARQUET HAS ALREADY BEEN CREATED, IF NOT:
file_path = Path("2024-citibike-tripdata-full.parquet")
if (not (file_path.exists())):

    for year in list_of_years: #iterate through the years to convert csv to parquet
        csv_path = list_of_source_files_csv[year]

        duck_citibike_connect.execute(f"""
                COPY (
                    SELECT *
                    FROM read_csv_auto(
                    '{csv_path}',
                    types={{'column05': 'VARCHAR'}}
                )
            ) TO '{year}-citibike-tripdata-full.parquet' (FORMAT PARQUET);
        """)


for year in list_of_years: #iterate through the years to create the parquet file veriable names

    list_of_source_files_parquet[year] = Path(f"{year}-citibike-tripdata-full.parquet") #ex: 2024-citibike-tripdata-full.parquet



for year in list_of_years: #iterate through the years to load the parquet into duckdb SQL for each year
    table_name = list_of_tables[year]
    parquet_path = list_of_source_files_parquet[year]


    # IF ALREADY EXISTS, BUT DATA IS OLD, USER RESPONSIBILITY TO REPLACE
    duck_citibike_connect.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} AS 
        SELECT *
        FROM read_parquet('{parquet_path}')
    """)

#ACTUAL ANALYSIS BEGINS HERE

#NTA table in DuckDB database
duck_citibike_connect.execute("""
    CREATE OR REPLACE TABLE ntas AS
    SELECT *
    FROM ST_Read('NTAs/geo_export_bd71e9cd-14bd-435b-99c8-b573a94a1dc8.shp');
""")

neighborhood_list = duck_citibike_connect.execute("DESCRIBE ntas").fetchall()

for year in list_of_years: #iterate through the years to read/process duckdb tables
    table_name = list_of_tables[year]

    total_count = duck_citibike_connect.execute(f"""
        SELECT COUNT(*) FROM {table_name} 
    """).fetchone()[0] #take total bike counts

    ebike_count = duck_citibike_connect.execute(f"""
        SELECT COUNT(*) FROM {table_name} 
        WHERE column01 = 'electric_bike'
    """).fetchone()[0] #take ebike counts

    classicbike_count = duck_citibike_connect.execute(f"""
        SELECT COUNT(*) FROM {table_name} 
        WHERE column01 = 'classic_bike'
    """).fetchone()[0] #take classic bike counts

    print(f"{year}: {total_count:,} total bikes")
    print(f"{year}: {ebike_count:,} e-bikes")
    print(f"{year}: {classicbike_count:,} classic bikes")


print(neighborhood_list)

#CLOSE DATABASE
duck_citibike_connect.close()