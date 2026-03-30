# make sure to install these packages before running:
# pip install pandas
# pip install sodapy
# pip install duckdb

import duckdb #DuckDB will process the data with SQL
import pandas as pd #pandas to work with smaller result summary tables
from sodapy import Socrata #socrata is used to pull and organize data from NYS opendata
import time #to sleep between requests
import requests #to manage multiple requests via API if first attempt breaks

#list the datasets in use for the website project and define their NYS Open Data Codes:
open_data_dict = {
    "mta_bridge_traffic": "ebfx-2m7v",
    "mta_subway_ridership": "5wq4-mkjj",
    "cbd_entries": "t6yz-b64h",
    "mta_overall_ridership_traffic": "sayj-mze2"
}

#initialize DuckDB database for the report card
duck_report_card_connect = duckdb.connect(database="/home/doke30/urban blogs/UrbanBlogs/src/report_card.duckdb") 

#use spatial element
duck_report_card_connect.execute("INSTALL spatial;")
duck_report_card_connect.execute("LOAD spatial;")

# create table "mta_bridge_traffic", define the columns from the dataset
# enforce uniqueness in table
# https://dev.socrata.com/foundry/data.ny.gov/ebfx-2m7v 
duck_report_card_connect.execute(f"""
    CREATE TABLE IF NOT EXISTS mta_bridge_traffic (
        transit_timestamp TIMESTAMP,
        date DATE,
        hour INTEGER,
        facility_id TEXT,
        facility TEXT,
        direction TEXT,
        payment_method TEXT,
        vehicle_class INTEGER,
        vehicle_class_description TEXT,
        vehicle_class_category TEXT,
        traffic_count INTEGER,
        CONSTRAINT unique_row UNIQUE (transit_timestamp, facility, direction, payment_method, vehicle_class)    
    );
""")

# create table "mta_subway_ridership", define the columns from the dataset
# enforce uniqueness in table
# https://dev.socrata.com/foundry/data.ny.gov/5wq4-mkjj
duck_report_card_connect.execute(f"""
    CREATE TABLE IF NOT EXISTS mta_subway_ridership (
        transit_timestamp TIMESTAMP,
        transit_mode TEXT,
        station_complex_id TEXT,
        station_complex TEXT,
        borough TEXT,
        payment_method TEXT,
        fare_class_category TEXT,
        ridership FLOAT,
        transfers FLOAT,
        latitude FLOAT,
        longitude FLOAT,
        georeference GEOMETRY,
        CONSTRAINT unique_row UNIQUE(transit_timestamp, station_complex_id, transit_mode, payment_method, fare_class_category)
    );
""")

# create table "cbd_entries", define the columns from the dataset
# enforce uniqueness in table
# https://dev.socrata.com/foundry/data.ny.gov/t6yz-b64h
duck_report_card_connect.execute(f"""
    CREATE TABLE IF NOT EXISTS cbd_entries (
        toll_date TIMESTAMP,
        toll_hour TIMESTAMP,
        toll_10_minute_block TIMESTAMP,
        minute_of_hour FLOAT,
        hour_of_day FLOAT,
        day_of_week_int FLOAT,
        day_of_week TEXT,
        toll_week TIMESTAMP,
        time_period TEXT,
        vehicle_class TEXT,
        detection_group TEXT,
        detection_region TEXT,
        crz_entries FLOAT,
        excluded_roadway_entries FLOAT,
        CONSTRAINT unique_row UNIQUE(toll_10_minute_block, vehicle_class, detection_group, detection_region)
    );
""")

# create table "mta_overall_ridership_traffic", define the columns from the dataset
# enforce uniqueness in table
# https://dev.socrata.com/foundry/data.ny.gov/sayj-mze2
duck_report_card_connect.execute(f"""
    CREATE TABLE IF NOT EXISTS mta_overall_ridership_traffic (
        date TIMESTAMP,
        mode TEXT,
        count FLOAT,
        CONSTRAINT unique_row UNIQUE(date, mode)
    );
""")

# Unauthenticated client only works with public data sets. Note 'None' <- DEPRECATED UNAUTHENTICATED API CLIENT, DO NOT USE
# in place of application token, and no username or password:
# client = Socrata("data.ny.gov", None, timeout=60)

#Authenticated client:
# open the api login text file to pass through login credentials
# avoids listing login credentials directly in program file, which is bad for security
# (CONSIDER ENCRYPTING LOGIN FILE / HASHING FOR WEB DEPLOYMENT) 
with open("api_login.txt") as f:
    token = f.readline().strip()
    user = f.readline().strip()
    password = f.readline().strip()

# load source site, user token + name + password, max timeout of 60s to avoid crashing before median pull time of ~40s
nys_client = Socrata(
    "data.ny.gov",
    token,
    user,
    password,
    timeout=60
)

#declare size for how big of an api request to make at one time
api_pull_size = 200000


# #TEMPORARY, REMOVE LATER
# temp_row_count = 0

#function for the timestamp name used for each nys opendata dataset
def get_timestamp_name(duckdb_database):
    return ({
        "mta_bridge_traffic" : "transit_timestamp",
        "mta_subway_ridership" : "transit_timestamp",
        "cbd_entries" : "toll_10_minute_block",
        "mta_overall_ridership_traffic" : "date"
    }.get(duckdb_database)
    or "none_found")

#importing georeference will be in the following format:
    # type VARCHAR,
    # coordinates DOUBLE[]
# this MUST be converted to WKT point geometry to work as a GEOMETRY type.
def convert_georeference_to_wkt(row):
    geo_text = row.get("georeference")

    if geo_text and "coordinates" in geo_text:
        lon, lat = geo_text["coordinates"]
        return f"POINT ({lon} {lat})"
    else:
        return None

#function to retrieve all rows from a given dataset:
    # takes in the client credentials, desired dataset, and desired output database
    # hard limit of 200k rows per refresh, refreshes until no more data to pull
def update_duckdb_database(client, dataset, duckdb_database, limit=api_pull_size):

    # #TEMPORARY, REMOVE LATER
    # global temp_row_count

    #get the latest timestamp. only pull data that is newer than the latest timestamp in order to not get repeat data

    latest_timestamp = duck_report_card_connect.execute(f"""
        SELECT MAX({get_timestamp_name(duckdb_database)}) FROM {duckdb_database}
    """).fetchone()[0]

    
    #initialize deliberately single-row dataset for now before first pull
    rows = ["hi"]

    #if the previous pull was full size, or if there hasn't been a pull yet, loop through pulls
    while ((len(rows) == 1) or (len(rows) == api_pull_size)): #<- loop

        #if no latest timestamp, data is not in database yet, make up some date that is impossible to precede to pull oldest data first
        if ((latest_timestamp is None) and (rows == ["hi"])):
            latest_timestamp = "1900-01-01T00:00:00"
        elif hasattr(latest_timestamp, "strftime"):
            #convert timestamp to proper timestamp format
            latest_timestamp = latest_timestamp.strftime("%Y-%m-%dT%H:%M:%S")
        
        # print(latest_timestamp)

        #max of 5 https API requests per run
        max_retries = 5
        for attempt in range(max_retries):

            try:
                #get data only if the date is more recent than our most recent available data.
                #obtain data in chronological order: oldest -> newest
                rows = client.get(dataset, limit=limit, where=f"{get_timestamp_name(duckdb_database)} >= '{latest_timestamp}'", order=f"{get_timestamp_name(duckdb_database)} ASC")

                break
            
            #catch exception for misencoding https request
            except requests.exceptions.ChunkedEncodingError:
                print(f"ChunkedEncodingError, retrying {attempt+1}/{max_retries}...")
                time.sleep(2 ** attempt)
            
            #catch exception for timeouts, allow retries on timeouts
            except requests.exceptions.ReadTimeout:
                print(f"ReadTimeout, retrying {attempt+1}/{max_retries}...")
                time.sleep(2 ** attempt)

        else: 
            #after multiple failed attempts, finally throw the error
            raise RuntimeError("Failed after multiple request retries")


        if not rows:
            break #<- until nothing is left

        #put each chunk into a pandas df
        df_rows = pd.DataFrame.from_records(rows)

        #for databases with georeferences, convert them properly to WKT geometry before adding
        if "georeference" in df_rows.columns:
            df_rows["georeference"] = df_rows["georeference"].map(convert_georeference_to_wkt)

        #get columns dynamically
        table_columns = [row[0] for row in duck_report_card_connect.execute(f"DESCRIBE {duckdb_database}").fetchall()]

        #ensure df_rows has the same columns and order
        df_rows = df_rows.reindex(columns=table_columns)

        #update latest_timestamp using rows because we won't get to see those new rows in our duckdb database for a while-- we have yielded them            
        duck_report_card_connect.register("df_rows_view", df_rows)

        latest_timestamp = duck_report_card_connect.execute(f"""
            SELECT MAX({get_timestamp_name(duckdb_database)}) FROM df_rows_view
        """).fetchone()[0]

        # #TEMPORARY, REMOVE LATER
        # temp_row_count += len(rows)
        # print(temp_row_count)

        duck_report_card_connect.execute(f"""
            INSERT INTO {duckdb_database}
            SELECT *
            FROM df_rows_view
            ON CONFLICT DO NOTHING
        """)
    
    return        

#run function for CBD Entries
update_duckdb_database(nys_client, open_data_dict["cbd_entries"], "cbd_entries")

#run function for MTA Bridge Traffic
update_duckdb_database(nys_client, open_data_dict["mta_bridge_traffic"], "mta_bridge_traffic")

#run function for MTA Overall Ridership/Traffic
update_duckdb_database(nys_client, open_data_dict["mta_overall_ridership_traffic"], "mta_overall_ridership_traffic")

# run function for MTA Subway Ridership
# update_duckdb_database(nys_client, open_data_dict["mta_subway_ridership"], "mta_subway_ridership")
# ^^Too big, drop for now

for metric in ["cbd_entries", "mta_bridge_traffic", "mta_overall_ridership_traffic", "mta_subway_ridership"]:

    traffic_row_count = duck_report_card_connect.execute(f"""
        SELECT COUNT(*) FROM {metric} AS traffic_row_count
    """).fetchone()[0]

    first_thousand = duck_report_card_connect.execute(f"""
        SELECT * FROM {metric}
        ORDER BY {get_timestamp_name(metric)} DESC
        LIMIT 3
        """).fetchall()
    
    print(traffic_row_count)
    print(first_thousand)




duck_report_card_connect.close()