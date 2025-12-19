import pandas as pd #table reading library: Pandas
from pathlib import Path # we use Path to establish a location for the folder where our collection of monthly data files lives
from matplotlib import pyplot as plt #plotting library to graph info visually
from enum import Enum #make enumerated keywords (words with numerical values)

COMPARE_STYLE = ("HOURLY", "MONTHLY") #list of possible ways to compare data periods
COMPARE_DECISION = COMPARE_STYLE[1] #decision index

DATE_STYLE = ("weekend", "non-holiday weekday", "holiday") #list of possible ways to choose types of days to compare
DATE_DECISION = DATE_STYLE[1] #deciaion index

month_list = ["01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12"]

#TEMPLATE TO FILL IN VARIABLE NAMES BY YEAR
list_of_years = ["2024", "2025"] #EDIT HERE TO UPDATE THE LIST OF YEARS TO CHECK
list_of_source_files = {} #source file dictionary, keeps track of source files through the years
list_of_dfs = {} #df dictionary, keeps track of dfs through the months and years
list_of_grouped_hourly_dfs = {} #same as above, but grouped by hour
list_of_grouped_monthly_dfs = {} #same as above, but grouped by month

super_df = pd.DataFrame()

for year in list_of_years: #iterate through the years to create the variable names

    globals()[f"source_files_{year}"] = sorted(Path(f'{year}/').glob('*.xls'))
    list_of_source_files[year] = globals()[f"source_files_{year}"]

    for month in range(1, 13, 1): #iterate through the months to make the monthly df names
        month_text = str(month).zfill(2) #convert month numbers to string "01, 02", etc. to be 2-digit
        globals()[f"df_{month_text}/{year}"] = pd.DataFrame()
        list_of_dfs[f"{month_text}/{year}"] = globals()[f"df_{month_text}/{year}"]

        
#list of nyc holidays
nyc_holidays = [
"2024-01-01","2024-01-15","2024-02-12","2024-02-19","2024-05-27","2024-06-19","2024-07-04",
"2024-09-02","2024-10-14","2024-11-05","2024-11-11","2024-11-28","2024-12-25",
"2025-01-01","2025-01-20","2025-02-12","2025-02-17","2025-05-26","2025-06-19","2025-07-04",
"2025-09-01","2025-10-13","2025-11-04","2025-11-11","2025-11-27","2025-12-25"
]

#convert to datetime element
nyc_holidays = pd.to_datetime(nyc_holidays)

def classify_day(date): #function to classify non-holiday weekdays, weekends, and holidays
    if date in nyc_holidays:
        return "holiday"
    elif date.weekday() >= 5:  # 5=Saturday, 6=Sunday
        return "weekend"
    else:
        return "non-holiday weekday"

def clean_month(df): #function to create a quite frankly better organized monthly df than whatever nightmare we started with
    # desired format: 
    # direction | date | hour | count

    df = df.copy() #safety copy

    #drop rows after 113
    df = df.head(114) #keep only rows 0-113

    #assign new direction label column
    labels = (["All"] * 38) + (["Northbound"] * 38) + (["Southbound"] * 38) #each hour is -> 0-37 = all;
    df["direction"] = labels                                                               # 38-75 = north;
                                                                                           # 76-110 = south
    #drop summary rows at the bottom (rows where 'Unnamed: 0_level_2' is NaN or not a time)
    df = df[df['Unnamed: 0_level_2'].str.match(r'\d{2}:\d{2}:\d{2}', na=False)] # <- regex searching for xx:xx:xx (time), and we're also cutting "NaN" rows out

    #drop summary columns
    df = df.loc[:, ~df.columns.str.contains('Workday|7 Day|Count')]

    #change "unnamed" column to hour column
    df = df.rename(columns={'Unnamed: 0_level_2': 'hour'})

    #delineate non-date and date attributes
    id_vars = ['hour', 'direction'] # <- non date variables that we need to protect
    date_vars = [col for col in df.columns if col not in id_vars] # <- date columns

    #melt date columns into long format
    df_long = df.melt(id_vars=id_vars, value_vars=date_vars, var_name='date', value_name='count')

    #convert 'date' to datetime and 'count' to float
    df_long['date'] = pd.to_datetime(df_long['date'])
    df_long['count'] = df_long['count'].astype(float)

    #new column that indicates type of day in df_long
    df_long['day_type'] = df_long['date'].apply(classify_day)

    #new column that makes a valid/invalid check based on if any hours are missing data (if ANY hours are missing data, the entire day is gone)
    validity_map = df_long.groupby('date')['count'].apply(lambda x: 'Valid' if x.notna().all() else 'Invalid')

    #merge back into df_long so every hour gets the same validity for that date
    df_long['validity'] = df_long['date'].map(validity_map)

    df_long['month'] = df_long['date'].dt.month
    df_long['day'] = df_long['date'].dt.day
    df_long['year'] = df_long['date'].dt.year

    return df_long

                                                                                    
for year in list_of_years: #iterate through the years, FILE HANDLING/REMOVING JUNK DATA

    for file in list_of_source_files[year]: #iterate through each monthly file in the year, named "file"

        all_tables = pd.read_html(file) # read the monthly file

        df = max(all_tables, key=lambda t: t.shape[0] * t.shape[1])  # select the largest table in the html file; largest = main data

        second_level = df.columns.get_level_values(2) # "columns" in table are nested inside a wrapper, get them out of the wrapper

        date_cols = [col for col in second_level if col.startswith(str(year))] #select only the columns with dates to get the month of the current table

        first_date = pd.to_datetime(date_cols[0]) # the first column has the first date
        month_text = str(first_date.month).zfill(2) #convert month numbers of the first date to string "01, 02", etc. to be 2-digit

        list_of_dfs[f"{month_text}/{year}"] = df #our list of dfs is now composed of the monthly tables


#continue modifying the structure/format of the monthly data, it will be nice and clean at the end
for year in list_of_years:
    for month in month_list:
        if not (year == "2025" and month in ["12"]): #only attempt clean for months that exist

            #only apply if columns are a MultiIndex
            if isinstance(list_of_dfs[f"{month}/{year}"].columns, pd.MultiIndex):

                #drop "All directions" and the weekday level
                list_of_dfs[f"{month}/{year}"].columns = list_of_dfs[f"{month}/{year}"].columns.droplevel([0, 1])

            #now df.columns should be like:
            #['2025-10-01', '2025-10-02', ..., 'Workday', '7 Day', 'Count']

            list_of_dfs[f"{month}/{year}"] = clean_month(list_of_dfs[f"{month}/{year}"])

            super_df = pd.concat([super_df, list_of_dfs[f"{month}/{year}"]], ignore_index=True)

        else: #if months aren't there, delete the dfs for those months

            del list_of_dfs[f"{month}/{year}"]


# print(list_of_dfs)

# super_df.to_excel("cbe_2024_2025_disagg.xlsx", index=False)  


#type of data we want

    # monthly weekend             YOY
    # monthly non-holiday weekday YOY
    # monthly holiday             YOY

    # hourly weekend              YOY
    # hourly non-holiday weekday  YOY
    # hourly holiday              YOY

    # ALL/NB/SB FOR ALL OF THE ABOVE!!! ^^

#DAILY

daily_valid_df = super_df[
    (super_df['validity'] == 'Valid') & #is valid
    (super_df['day_type'].isin([DATE_STYLE[0], DATE_STYLE[1]])) # & is weekend or non holiday weekday
    # (super_df['direction'] == 'All')
].groupby(['year', 'month', 'day', 'direction', 'day_type'], as_index=False)['count'].sum()

# daily_valid_df = daily_valid_df.sort_values(by=['direction', 'year', 'month', 'day', 'day_type'])

#MONTHLY

monthly_avg_df = daily_valid_df.groupby(['direction', 'day_type', 'year', 'month'], as_index=False)['count'].mean()


monthly_avg_side_by_side_YOY_df = monthly_avg_df.pivot(
    index=['direction', 'day_type', 'month'],
    columns='year',
    values='count'
)

monthly_avg_side_by_side_YOY_df = monthly_avg_side_by_side_YOY_df.reset_index()

monthly_avg_side_by_side_YOY_df['YOY %'] = (
    (monthly_avg_side_by_side_YOY_df[2025] - monthly_avg_side_by_side_YOY_df[2024])
    / monthly_avg_side_by_side_YOY_df[2024].replace(0, pd.NA) # handles NA issues (unfinished months)
    * 100
).round(2)


#HOURLY

hourly_valid_df = super_df[
    (super_df['validity'] == 'Valid') & #is valid
    (super_df['day_type'].isin([DATE_STYLE[0], DATE_STYLE[1]])) # & is weekend or non holiday weekday
    # (super_df['direction'] == 'All')
].groupby(['direction', 'day_type', 'year', 'hour'], as_index=False)['count'].mean()


hourly_valid_side_by_side_YOY_df = hourly_valid_df.pivot(
    index=['direction', 'day_type', 'hour'],
    columns='year',
    values='count'    
)

hourly_valid_side_by_side_YOY_df = hourly_valid_side_by_side_YOY_df.reset_index()

hourly_valid_side_by_side_YOY_df['YOY %'] = (
    (hourly_valid_side_by_side_YOY_df[2025] - hourly_valid_side_by_side_YOY_df[2024])
    / hourly_valid_side_by_side_YOY_df[2024].replace(0, pd.NA) # handles NA issues (unfinished months)
    * 100
).round(2)

# print(daily_valid_df)
# print(hourly_valid_side_by_side_YOY_df)
# print(monthly_avg_side_by_side_YOY_df)

# daily_valid_df.to_excel("cbe_2024_2025_agg_daily.xlsx", index=False)  
# hourly_valid_side_by_side_YOY_df.to_excel("cbe_2024_2025_agg_hourly.xlsx", index=False)  
# monthly_avg_side_by_side_YOY_df.to_excel("cbe_2024_2025_agg_monthly.xlsx", index=False)  


