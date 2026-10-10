import requests
import pandas as pd
from pandas.tseries.holiday import USFederalHolidayCalendar

tracker_location_dict = {
    "MNHBR": '100062893',
    "BKBR":  '300020904',
    "WLB":   '100009427',
    "QNSBR": '100009428',
}

def pull_bike_counts_location(tracker_location, time_style):

    url = "https://data.cityofnewyork.us/resource/ct66-47at.json"

    # AND timestamp < '2026-01-01T00:00:00'
    # vv

    tracker_code = tracker_location_dict[tracker_location]

    params = {
        "$where": f"""
            sensor_id = '{tracker_code}'
            AND timestamp >= '2018-01-01T00:00:00'
        """,
        "$order": "timestamp",
        "$limit": 1000000
    }

    response = requests.get(url, params=params)
    response.raise_for_status()

    data = response.json()

    # print("Number of records:", len(data))

    df = pd.DataFrame(data)

    # print(df.columns.tolist())
    # print(df.head())
    # print(df["granularity"].value_counts())


    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df["counts"] = pd.to_numeric(df["counts"])

    monthly = None
    yearly = None

    if time_style in ("monthly", "all"):
        monthly = (
            df.groupby([
                df["timestamp"].dt.to_period("M"),
                "direction"
            ])["counts"]
            .sum()
            .reset_index()
        )

        monthly.columns = ["month", "direction", "counts"]

        print("Monthly data for ", tracker_location, ": ", len(monthly), " entries.")

        monthly.sort_values(["direction", "month"]).to_csv(
            f"{tracker_location}_monthly_bike_count.csv",
            index=False
        )

    if time_style in ("yearly", "all"):
        yearly = (
            df.groupby([
                df["timestamp"].dt.to_period("Y"),
                "direction"
            ])["counts"]
            .sum()
            .reset_index()
        )

        yearly.columns = ["year", "direction", "counts"]

        print("Yearly data for ", tracker_location, ": ", len(yearly), " entries.")

        yearly.sort_values(["direction", "year"]).to_csv(
            f"{tracker_location}_yearly_bike_count.csv",
            index=False
        )

    if time_style in ("average_weekday_monthly", "all"):

        daily = (
            df.groupby([
                df["timestamp"].dt.normalize(),
                "direction"
            ])["counts"]
            .sum()
            .reset_index()
        )
        daily.columns = ["date", "direction", "counts"]


        daily = daily[daily["date"].dt.dayofweek < 5]


        holidays = pd.tseries.holiday.USFederalHolidayCalendar().holidays(
            start=daily["date"].min(),
            end=daily["date"].max()
        )
        daily = daily[~daily["date"].isin(holidays)]


        avg_weekday = (
            daily.groupby([
                daily["date"].dt.to_period("M"),
                "direction"
            ])["counts"]
            .mean()
            .reset_index()
        )

        avg_weekday.columns = ["month", "direction", "avg_weekday_counts"]

        print("Avg weekday monthly data for", tracker_location, ":", len(avg_weekday), "entries.")

        avg_weekday.sort_values(["direction", "month"]).to_csv(
            f"{tracker_location}_average_weekday_monthly_bike_count.csv",
            index=False
        )

    if time_style in ("average_weekday_yearly", "all"):

        daily = (
            df.groupby([
                df["timestamp"].dt.normalize(),
                "direction"
            ])["counts"]
            .sum()
            .reset_index()
        )
        daily.columns = ["date", "direction", "counts"]


        daily = daily[daily["date"].dt.dayofweek < 5]


        holidays = pd.tseries.holiday.USFederalHolidayCalendar().holidays(
            start=daily["date"].min(),
            end=daily["date"].max()
        )
        daily = daily[~daily["date"].isin(holidays)]


        avg_weekday = (
            daily.groupby([
                daily["date"].dt.to_period("Y"),
                "direction"
            ])["counts"]
            .mean()
            .reset_index()
        )

        avg_weekday.columns = ["year", "direction", "avg_weekday_counts"]

        print("Avg weekday yearly data for", tracker_location, ":", len(avg_weekday), "entries.")

        avg_weekday.sort_values(["direction", "year"]).to_csv(
            f"{tracker_location}_average_weekday_yearly_bike_count.csv",
            index=False
        )

# pull_bike_counts_location("BKBR", "yearly") # <- MNHNBR, YEARLY

for key in tracker_location_dict:
    pull_bike_counts_location(key, "all")