import duckdb #DuckDB will process the data with SQL
from pathlib import Path # we use Path to establish a location for the folder where our collection of monthly data files lives
from matplotlib import pyplot as plt #plotting library to graph info visually
from enum import Enum #make enumerated keywords (words with numerical values)
import pandas as pd #pandas to work with smaller result summary tables

#TEMPLATE TO FILL IN VARIABLE NAMES BY YEAR
list_of_years = ["2015", "2016", "2017", "2018", "2019", "2020", "2021", "2022", "2023", "2024", "2025"] #EDIT HERE TO UPDATE THE LIST OF YEARS TO CHECK


#CREATE DATABASE, WILL BE CLOSED AT THE END OF RUN
duck_ems_connect = duckdb.connect(database="ems.duckdb") 




#CLOSE DATABASE
duck_ems_connect.close()
