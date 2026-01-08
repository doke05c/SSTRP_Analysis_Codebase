import duckdb #DuckDB will process the data with SQL
from pathlib import Path # we use Path to establish a location for the folder where our collection of monthly data files lives
from matplotlib import pyplot as plt #plotting library to graph info visually
from enum import Enum #make enumerated keywords (words with numerical values)
import pandas as pd #pandas to work with smaller result summary tables

#TEMPLATE TO FILL IN VARIABLE NAMES BY YEAR
list_of_years = [2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024, 2025] #EDIT HERE TO UPDATE THE LIST OF YEARS TO CHECK


#CREATE DATABASE, WILL BE CLOSED AT THE END OF RUN
duck_ems_connect = duckdb.connect(database="ems.duckdb") 

#MAKE NAME FOR SQL/DUCKDB TABLE (makes port from previous analysis more convenient)
table_name = "EMS_TABLE"

#CHECK IF PARQUET HAS ALREADY BEEN CREATED, IF NOT, MAKE IT, AND THE TABLE:
parquet_path = Path("ems_response_since_2015.parquet")
csv_path = Path("ems_response_since_2015.csv")

if (not (parquet_path.exists())):

    duck_ems_connect.execute(f"""
            COPY (
                SELECT *
                FROM read_csv_auto(
                '{csv_path}'
            )
        ) TO '{parquet_path}' (FORMAT PARQUET);
    """)

    #CREATE DUCKDB TABLE
    duck_ems_connect.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} AS 
        SELECT *
        FROM read_parquet('{parquet_path}')
    """)

    #RECLASSIFY TRAVEL TIMES AS INTEGERS
    duck_ems_connect.execute(f"""
    ALTER TABLE {table_name}
    ADD COLUMN INCIDENT_TRAVEL_TM_SECONDS_QY_INT INTEGER;
    """)

    duck_ems_connect.execute(f"""
    ALTER TABLE {table_name}
    ADD COLUMN INCIDENT_RESPONSE_SECONDS_QY_INT INTEGER;
    """)

    duck_ems_connect.execute(f"""
    UPDATE {table_name}
    SET
        INCIDENT_TRAVEL_TM_SECONDS_QY_INT =
            TRY_CAST(REPLACE(INCIDENT_TRAVEL_TM_SECONDS_QY, ',', '') AS INTEGER),
        INCIDENT_RESPONSE_SECONDS_QY_INT =
            TRY_CAST(REPLACE(INCIDENT_RESPONSE_SECONDS_QY, ',', '') AS INTEGER);
    """)

#ACTUAL ANALYSIS BEGINS HERE

#FIND OUT HOW MANY OF THE ROWS ARE ZEROES
validity_results = duck_ems_connect.execute(f"""
    SELECT
        EXTRACT(YEAR FROM INCIDENT_DATETIME) AS year,

        SUM(CASE
            WHEN INCIDENT_TRAVEL_TM_SECONDS_QY_INT = 0
            THEN 1 ELSE 0 END
        ) AS invalid_travel_time_count,

        SUM(CASE
            WHEN INCIDENT_RESPONSE_SECONDS_QY_INT = 0
            THEN 1 ELSE 0 END
        ) AS invalid_response_time_count,

        COUNT(*) AS total_entries

        FROM {table_name}
        GROUP BY year
        ORDER BY year 

""").fetchdf()

validity_results["invalid_travel_time_count_pct"] = validity_results["invalid_travel_time_count"] / validity_results["total_entries"] * 100

print(validity_results)


#WHAT IS THE AVERAGE TRAVEL/RESPONSE TIME, GROUPED BY YEAR/MONTH AND AREA
travel_time_results = duck_ems_connect.execute(f"""
    SELECT
        EXTRACT(YEAR FROM INCIDENT_DATETIME) AS year,
        EXTRACT(MONTH FROM INCIDENT_DATETIME) AS month,

        CASE
            WHEN INCIDENT_DISPATCH_AREA IN ('M1','M2','M3') THEN 'cbd'
            WHEN INCIDENT_DISPATCH_AREA IN ('M4','M5','M6','M7','M8','M9') THEN 'non_cbd_mnh'
            ELSE 'other'
        END AS area_type,

        CASE
            WHEN INITIAL_SEVERITY_LEVEL_CODE IN ('1', '2', '3') THEN 'life_threat'
            ELSE 'non_life_threat'
        END AS init_severity,

        AVG(
            CASE
                WHEN INCIDENT_TRAVEL_TM_SECONDS_QY_INT != 0
                THEN INCIDENT_TRAVEL_TM_SECONDS_QY_INT
                ELSE NULL
            END
        ) AS average_travel_time,

        AVG(
            CASE
                WHEN INCIDENT_RESPONSE_SECONDS_QY_INT != 0
                THEN INCIDENT_RESPONSE_SECONDS_QY_INT
                ELSE NULL
            END
        ) AS average_response_time

        FROM {table_name}
        GROUP BY year, month, area_type, init_severity
        ORDER BY year, month, area_type, init_severity;
""").fetchdf()


print(travel_time_results)

#MAKE CBD ONLY VERSION OF THE ABOVE DATASET SPLIT BY SEVERITY, PREPARE FOR GRAPHING (CONSIDER DOING THE SAME FOR OTHER AREAS)
travel_time_results_cbd_only_life_threat = travel_time_results[(travel_time_results['area_type'] == 'cbd') & (travel_time_results['init_severity'] == 'life_threat')].copy()
travel_time_results_cbd_only_non_life_threat = travel_time_results[(travel_time_results['area_type'] == 'cbd') & (travel_time_results['init_severity'] == 'non_life_threat')].copy()

#give year/month attribute
travel_time_results_cbd_only_non_life_threat['year_month'] = pd.to_datetime(travel_time_results_cbd_only_non_life_threat['year'].astype(str) + '-' + travel_time_results_cbd_only_non_life_threat['month'].astype(str) + '-01')
travel_time_results_cbd_only_life_threat['year_month'] = pd.to_datetime(travel_time_results_cbd_only_life_threat['year'].astype(str) + '-' + travel_time_results_cbd_only_life_threat['month'].astype(str) + '-01')


#MAKE GRAPH FOR CBD AVERAGE RESPONSE/TRAVEL TIME + SEVERITY
plt.figure(figsize=(14,6))
plt.plot(travel_time_results_cbd_only_life_threat['year_month'], travel_time_results_cbd_only_life_threat['average_travel_time'], marker='o', label='Average Travel Time for Life-Threatening (s)')
plt.plot(travel_time_results_cbd_only_life_threat['year_month'], travel_time_results_cbd_only_life_threat['average_response_time'], marker='o', label='Average Response Time for Life-Threatening (s)')

plt.plot(travel_time_results_cbd_only_non_life_threat['year_month'], travel_time_results_cbd_only_non_life_threat['average_travel_time'], marker='o', label='Average Travel Time for Non Life-Threatening (s)')
plt.plot(travel_time_results_cbd_only_non_life_threat['year_month'], travel_time_results_cbd_only_non_life_threat['average_response_time'], marker='o', label='Average Response Time for Non Life-Threatening (s)')

plt.title('CBD Average EMS Travel and Response Times by Month/Year and by Severity')
plt.xlabel('Month')
plt.ylabel('Seconds')
plt.legend()
plt.grid(True)
plt.tight_layout()
plt.show()

#CLOSE DATABASE
duck_ems_connect.close()
