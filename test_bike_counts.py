import requests
import pandas as pd

url = "https://data.cityofnewyork.us/resource/ct66-47at.json"


        # AND timestamp < '2026-01-01T00:00:00' 
        # vv

params = {
    "$where": """
        sensor_id = '100062893'
        AND timestamp >= '2018-01-01T00:00:00'
    """,
    "$order": "timestamp",
    "$limit": 1000000
}

response = requests.get(url, params=params)
response.raise_for_status()

data = response.json()

print("Number of records:", len(data))

df = pd.DataFrame(data)

print(df.columns.tolist())
print(df.head())
print(df["granularity"].value_counts())


df["timestamp"] = pd.to_datetime(df["timestamp"])
df["counts"] = pd.to_numeric(df["counts"])

monthly = (
    df.groupby([
        df["timestamp"].dt.to_period("M"),
        "direction"
    ])["counts"]
    .sum()
    .reset_index()
)

monthly.columns = ["month", "direction", "counts"]

print(monthly)

yearly = (
    df.groupby([
        df["timestamp"].dt.to_period("Y"),
        "direction"
    ])["counts"]
    .sum()
    .reset_index()
)

yearly.columns = ["year", "direction", "counts"]

print(yearly)

monthly.sort_values(["direction", "month"]).to_csv(
    "monthly_bike_count.csv",
    index=False
)


yearly.sort_values(["direction", "year"]).to_csv(
    "yearly_bike_count.csv",
    index=False
)
