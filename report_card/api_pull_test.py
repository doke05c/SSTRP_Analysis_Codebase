# make sure to install these packages before running:
# pip install pandas
# pip install sodapy
# pip install duckdb

import duckdb #DuckDB will process the data with SQL
import pandas as pd #pandas to work with smaller result summary tables
from sodapy import Socrata #socrata is used to pull and organize data from NYS opendata

#list the datasets in use for the website project and define their NYS Open Data Codes:
open_data_dict = {
    "mta_bridge_traffic": "ebfx-2m7v",
    "mta_subway_ridership": "5wq4-mkjj"
}

#initialize DuckDB database for the report card
duck_report_card_connect = duckdb.connect(database="report_card.duckdb") 

#use spatial element
duck_report_card_connect.execute("INSTALL spatial;")
duck_report_card_connect.execute("LOAD spatial;")


# create table "mta_bridge_traffic", define the columns from the dataset
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
        traffic_count INTEGER
    )
""")
# create table "mta_subway_ridership", define the columns from the dataset
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
    georeference GEOMETRY
    )
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

#function to retrieve all rows from a given dataset:
    # takes in the client credentials, desired dataset, and desired output database
    # hard limit of 200k rows per refresh, refreshes until no more data to pull
def get_all_rows(client, dataset, duckdb_database, limit=200000):

    #get the latest timestamp. only pull data that is newer than the latest timestamp in order to not get repeat data
    latest_timestamp = duck_report_card_connect.execute(f"""
        SELECT MAX(date) FROM {duckdb_database}
    """).fetchone()[0]

    #if no latest timestamp, data is not in database yet, make up some date that is impossible to precede to pull oldest data first
    if latest_timestamp is None:
        latest_timestamp = "1900-01-01"

    #set offset for purposes of keeping track of which row we are on per-pull
    offset = 0
    while True: #<- loop
        #get data only if the date is more recent than our most recent available data.
        #obtain data in chronological order: oldest -> newest
        rows = client.get(dataset, limit=limit, offset=offset, where=f"date > '{latest_timestamp}'", order="date ASC")
        if not rows:
            break #<- until nothing is left

        yield rows #<-process data one piece at a time, easier on memory
        
        #offset updates after new rows get taken in, go back to start of the loop to process another chunk
        offset += len(rows)

def update_duckdb_database(client, dataset, duckdb_database, limit=200000):

    #put each chunk into a pandas df -> into the duckdb database at a time
    for chunk in get_all_rows(client, dataset, duckdb_database):
        df_chunk = pd.DataFrame.from_records(chunk)
        duck_report_card_connect.append(duckdb_database, df_chunk)

#run function for MTA Bridge Traffic
update_duckdb_database(nys_client, open_data_dict["mta_bridge_traffic"], "mta_bridge_traffic")

#run function for MTA Subway Ridership
update_duckdb_database(nys_client, open_data_dict["mta_subway_ridership"], "mta_subway_ridership")

for metric in ["mta_bridge_traffic", "mta_subway_ridership"]:

    traffic_row_count = duck_report_card_connect.execute(f"""
        SELECT COUNT(*) FROM {metric} AS traffic_row_count
    """).fetchone()[0]

    first_thousand = duck_report_card_connect.execute(f"""
        SELECT * FROM {metric}
        ORDER BY date DESC, transit_timestamp DESC
        LIMIT 10
        """).fetchall()

    print(traffic_row_count)
    print(first_thousand)


duck_report_card_connect.close()