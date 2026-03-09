# make sure to install these packages before running:
# pip install pandas
# pip install sodapy

import pandas as pd
from sodapy import Socrata

open_data_dict = {
    "mta_bridge_traffic": "ebfx-2m7v"
}

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
    password
)

# First 2000 results, returned as JSON from API / converted to Python list of
# dictionaries by sodapy.
results = nys_client.get(open_data_dict["mta_bridge_traffic"], limit=2000)

# Convert to pandas DataFrame
results_df = pd.DataFrame.from_records(results)

print(results_df)