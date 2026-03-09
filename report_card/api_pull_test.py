# make sure to install these packages before running:
# pip install pandas
# pip install sodapy
# pip install duckdb

import duckdb #DuckDB will process the data with SQL
import pandas as pd #pandas to work with smaller result summary tables
from sodapy import Socrata #socrata is used to pull and organize data from NYS opendata

open_data_dict = {
    "mta_bridge_traffic": "ebfx-2m7v"
}

duck_report_card_connect = duckdb.connect(database="report_card.duckdb") 


# create table "mta_bridge_traffic"
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

traffic_row_count = duck_report_card_connect.execute(f"""
    SELECT COUNT(*) FROM mta_bridge_traffic
""").fetchone()[0]

print(traffic_row_count)

# Unauthenticated client only works with public data sets. Note 'None'
# in place of application token, and no username or password:
# client = Socrata("data.ny.gov", None, timeout=60)

#Authenticated client (needed for non-public datasets):
with open("api_login.txt") as f:
    token = f.readline().strip()
    user = f.readline().strip()
    password = f.readline().strip()

nys_client = Socrata(
    "data.ny.gov",
    token,
    user,
    password,
    timeout=60
)

temp_row_count = 0 #TEMP TO HELP US COUNT TOTAL ROWS, REMOVE LATER

def get_all_rows(client, dataset, duckdb_database, limit=200000):

    global temp_row_count #TEMP TO HELP US COUNT TOTAL ROWS, REMOVE LATER

    latest_timestamp = duck_report_card_connect.execute(f"""
        SELECT MAX(date) FROM {duckdb_database}
    """).fetchone()[0]

    if latest_timestamp is None:
        latest_timestamp = "1900-01-01"

    offset = 0
    while True:
        rows = client.get(dataset, limit=limit, offset=offset, where=f"date > '{latest_timestamp}'")
        if not rows:
            break

        yield rows
        offset += len(rows)
        temp_row_count += len(rows) #TEMP TO HELP US COUNT TOTAL ROWS, REMOVE LATER
        print(f"TEST (REMOVE LATER) - Number of Rows now: {temp_row_count}") #TEMP TO HELP US COUNT TOTAL ROWS, REMOVE LATER

def update_duckdb_database(client, dataset, duckdb_database, limit=200000):

    for chunk in get_all_rows(client, dataset, duckdb_database):
        df_chunk = pd.DataFrame.from_records(chunk)
        duck_report_card_connect.append(duckdb_database, df_chunk)

update_duckdb_database(nys_client, open_data_dict["mta_bridge_traffic"], "mta_bridge_traffic")

traffic_row_count = duck_report_card_connect.execute(f"""
    SELECT COUNT(*) FROM mta_bridge_traffic
""").fetchone()[0]

print(traffic_row_count)

duck_report_card_connect.close()