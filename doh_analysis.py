import pandas as pd #table reading library: Pandas
from pathlib import Path # we use Path to establish a location for the folder where our collection of monthly data files lives
from matplotlib import pyplot as plt #plotting library to graph info visually
from enum import Enum #make enumerated keywords (words with numerical values)

class AQI(Enum):
    VALID_DATA_CUTOFF = 0.6  #defined as the minimum proportion of data retained for the month to be considered "lossless" 

GRAPH_STYLE = ("HOURLY", "MONTHLY") #list of possible ways to graph data
DECISION = GRAPH_STYLE[1] #decision index

SITE_SELECTION_STYLE = ("LIST", "ITERATE") #list of possible ways to select sites in hourly analysis
SITE_DECISION = SITE_SELECTION_STYLE[1] #deciaion index

class MonthRange(Enum):      #to help us select monthly ranges
    CBE_2024 = (1, 9)        # CBE's 2024 data is valid from Jan to Sep (incl.), until it drops out in October-November.
    ALL = (1, 12)            # includes all months
    JAN = (1, 1)
    FEB = (2, 2)
    MAR = (3, 3)
    APR = (4, 4)
    MAY = (5, 5)
    JUN = (6, 6)
    JUL = (7, 7)
    AUG = (8, 8)
    SEP = (9, 9)
    OCT = (10, 10)
    NOV = (11, 11)
    DEC = (12, 12)
    

def filter_by_month_range(df, month_range: MonthRange): # function that filters by monthly range
    start, end = month_range.value
    if start <= end:
        #ex: march–may
        return df[((pd.to_datetime(df['ObservationDate'])).dt.month >= start) & ((pd.to_datetime(df['ObservationDate'])).dt.month <= end)]
    else:
        #ex: december–february (wraps around year)
        return df[((pd.to_datetime(df['ObservationDate'])).dt.month >= start) | ((pd.to_datetime(df['ObservationDate'])).dt.month <= end)]


site_id_list = [ #"dictionary" (really just a 2d array) of site ids and their names. to be used for replacement
    ['36005NY11534', 'mott_haven'],
    ['36005NY11790', 'hunts_point_upper'],
    ['36005NY12387', 'cross_bronx_expwy'],
    ['36047NY07974', 'bq_expwy'],
    ['36061NY08454', 'manhattan_bridge'],
    ['36061NY08552', 'williamsburg_bridge'],
    ['36061NY08653', 'fdr'],
    ['36061NY09734', 'broadway_and_35th'],
    ['36061NY09929', 'midtown_west'],
    ['36061NY10130', 'queensboro_bridge'],
    ['36061NY12380', 'wheights_hamilton_bridge'],
    ['36081NY07615', 'van_wyck'],
    ['36081NY08198', 'glendale'],
    ['36081NY09285', 'queens_college_lie'],
    ['36085NY03820', 'si_expwy'],
    ['36085NY04805', 'port_richmond'],
    ['360470118', 'bushwick'],
    ['360850111', 'freshkills'],
    ['360470052', 'sunset_park_bqe'],
    ['360050112', 'hunts_point_lower'],
    ['360810120', 'maspeth_lie'],
    ['360810124', 'queens_college_2_lie'],
    ['360050080', 'highbridge-mteden'],
    ['360050110', 'longwood'],
    ['360610135', 'ccny'],
    ['360610115', 'wheights_washington_bridge']
]

order = [
    site_id_list[16][1],
    site_id_list[20][1],
    site_id_list[17][1],
    site_id_list[18][1],
    site_id_list[21][1],
    site_id_list[24][1],
    site_id_list[23][1],
    site_id_list[19][1],
    site_id_list[25][1],
    site_id_list[22][1],
    site_id_list[0][1],
    site_id_list[1][1],
    site_id_list[2][1],
    site_id_list[3][1],
    site_id_list[4][1],
    site_id_list[5][1],    
    site_id_list[6][1],    
    site_id_list[7][1],    
    site_id_list[8][1],    
    site_id_list[9][1],    
    site_id_list[10][1],
    site_id_list[11][1],
    site_id_list[12][1],
    site_id_list[13][1],
    site_id_list[14][1],
    site_id_list[15][1],

] # order of summary df to match site_coords sheet


site_id_list_df = pd.DataFrame(site_id_list, columns=["Site ID", "ID Name"]) #convert list to df
# site_id_list_df.to_excel("doh_site_coords.xlsx", index=False) #export to excel


#TEMPLATE TO FILL IN VARIABLE NAMES BY YEAR
list_of_years = ["2022", "2023", "2024", "2025"] #EDIT HERE TO UPDATE THE LIST OF YEARS TO CHECK
list_of_source_files = {} #source file dictionary, keeps track of source files through the years
list_of_dfs = {} #df dictionary, keeps track of dfs through the years
list_of_grouped_hourly_dfs = {} #same as above, but grouped by hour
list_of_grouped_monthly_dfs = {} #same as above, but grouped by month

for year in list_of_years: #iterate through the years to create the variable names

    globals()[f"source_files_{year}"] = sorted(Path(f'{year}/').glob('*.csv'))
    list_of_source_files[year] = globals()[f"source_files_{year}"]

    globals()[f"df_{year}"] = pd.DataFrame()
    list_of_dfs[year] = globals()[f"df_{year}"]


missing_check_summary_data = [] #sheet that shows amount of data available per site per month

summary_data_df_all_years = pd.DataFrame() #df of summary of pm2.5 data in the following format:
# SITE NAME         1-2024          2-2024          3-2024             4-2024           5-2024 .. ETC               PM2.5 
# ,,                    ,,              ,,              ,,                  ,,              ,,                         ,,


for year in list_of_years: #iterate through the years, now we are modifying the data

    for file in list_of_source_files[year]: #iterate through each monthly file in the year, named "file"

        #FILE HANDLING/REMOVING JUNK DATA
        df = pd.read_csv(file, header=0, sep=",") # read the monthly file, acknowledge first-line header, acknolwedge comma separation

        #SITE ID NAME HANDLING
        for location in site_id_list:
            df = df.replace({location[0]: location[1]}) #replace site IDs with names

        #CHECK THROUGH EACH SITE FOR VALIDITY
        for location in site_id_list:

            site_name = location[1]
            valid_count = len(df[df['SiteID'].isin([site_name])])

            missing_check_summary_data.append({ # add to the valid data tracking sheet

                "YearMonth": df['ObservationTimeUTC'][0][0:7],
                "Site": site_name,
                "ValidCount": valid_count,
                "Cutoff": AQI.VALID_DATA_CUTOFF.value * 24 * 30,
                "Valid?": valid_count >= AQI.VALID_DATA_CUTOFF.value * 24 * 30,
                "ValidPct": round(valid_count / (24 * 31) * 100, 2)

            })

            # if valid_count < (AQI.VALID_DATA_CUTOFF.value * 24 * 30):
            #     print("Not enough entries on " + site_name + " over " + df['ObservationTimeUTC'][0][0:7])

        #TIME HANDLING
        df = df.loc[:, ['SiteID', 'ObservationTimeUTC', 'Value']] #subset out only the columns you want
        df['ObservationTimeUTC'] = pd.to_datetime(df['ObservationTimeUTC'], utc=True) #convert column to computer-legible timestamp
        df['ObservationTimeET'] = df['ObservationTimeUTC'].dt.tz_convert('America/New_York') #convert to NYC time (handles DST automatically)

        df['ObservationDate'] = df['ObservationTimeET'].dt.date # split date/time column-- 1: get date column
        df['ObservationTimeET'] = df['ObservationTimeET'].dt.time # split date/time column-- 2: get time column

        list_of_dfs[year] = pd.concat([list_of_dfs[year], df]) #Add monthly df to yearly total df 

if (DECISION == "HOURLY"):
    for year in list_of_years:
        list_of_dfs[year] = filter_by_month_range(list_of_dfs[year], MonthRange.ALL) #filter months out based on site and comparison needs for the hourlies

for year in list_of_years:
    # print(list_of_dfs[year])
    list_of_dfs[year].to_csv(f'nyc_sanity_check_{year}.csv', index=False)

for year in list_of_years:

    #HOURLY GROUP
    globals()[f"grouped_hourly_df_{year}"] = list_of_dfs[year].groupby(['SiteID', 'ObservationTimeET'], as_index=False)['Value'].mean() #make a dataset that groups all the site data together, splitting by hour
    list_of_grouped_hourly_dfs[year] = globals()[f"grouped_hourly_df_{year}"]

    # print(list_of_grouped_hourly_dfs[year])

    #MONTHLY GROUP
    list_of_dfs[year]['Month'] = pd.to_datetime(list_of_dfs[year]['ObservationDate']).dt.month

    globals()[f"grouped_monthly_df_{year}"] = list_of_dfs[year].groupby(['SiteID', 'Month'], as_index=False)['Value'].mean() #make a dataset that groups all the site data together, splitting by month
    list_of_grouped_monthly_dfs[year] = globals()[f"grouped_monthly_df_{year}"]

    # print(list_of_grouped_monthly_dfs[year])

#convert summary data into a DataFrame
valid_check_df = pd.DataFrame(missing_check_summary_data)

#pivot so each month is a row (Y-axis), each site is a column (X-axis)
pivot_valid_check_df = valid_check_df.pivot_table(index="YearMonth", columns="Site", values="ValidPct", fill_value=0)

#export to excel
# pivot_valid_check_df.to_excel("monthly_validity_summary_all_years.xlsx")

if (SITE_DECISION == "LIST"):

    # site_to_plot = ("wheights_washington_bridge", "ccny")
    # site_to_plot = ("broadway_and_35th", "manhattan_bridge", "queensboro_bridge", "williamsburg_bridge", "fdr")
    site_to_plot = ("cross_bronx_expwy", "highbridge-mteden", "mott_haven", "longwood", "hunts_point_lower")

    for year in list_of_years:

        if (DECISION == "HOURLY"):

            df_to_plot = list_of_grouped_hourly_dfs[year][list_of_grouped_hourly_dfs[year]['SiteID'].isin(site_to_plot)].copy() #copy the original data to not damage it, by site
            # NOTE: WE DO NOT REMOVE "INVALID MONTHS" BECAUSE THE CONCEPT DOES NOT APPLY HERE-- WE CAN REMOVE CHUNKS OF NON PRESENT DATA, BUT WHEN IT GETS AGGREGATED INTO THE HOURLIES IT DOESN'T MAKE A DIFFERENCE

            df_to_plot = (df_to_plot.groupby('ObservationTimeET', as_index=False)['Value'].mean()) #compute average by hour across listed sites

            #PLOT HOURLY
            df_to_plot['Hour'] = df_to_plot['ObservationTimeET'].apply(lambda t: t.hour + t.minute/60 + t.second/3600) # convert the pandas time unit to a plt-readable hour unit
            plt.plot(df_to_plot['Hour'], df_to_plot['Value'], label=f'{year}', marker='o', linestyle='-') #plot hourly against pm2.5 concentration with a legend for each year, and lines and points

        if (DECISION == "MONTHLY"):

            df_list = []

            for site in site_to_plot:

                # select data for this site
                df_site = list_of_grouped_monthly_dfs[year][list_of_grouped_monthly_dfs[year]['SiteID'] == site].copy()

                # find valid months for this site and year
                valid_months_for_site = valid_check_df[
                    (valid_check_df['Site'] == site) &
                    (valid_check_df['YearMonth'].str.startswith(year)) &
                    (valid_check_df['Valid?'] == True)
                ]['YearMonth'].apply(lambda x: int(x.split('-')[1])).tolist()

                # keep only valid months
                df_site = df_site[df_site['Month'].isin(valid_months_for_site)]

                df_list.append(df_site)

            # combine all sites
            df_to_plot = pd.concat(df_list)

            # average across sites for each month
            df_to_plot = df_to_plot.groupby('Month', as_index=False)['Value'].mean()

            # PLOT MONTHLY
            plt.plot(df_to_plot['Month'], df_to_plot['Value'], label=f'{year}', marker='o', linestyle='-')
    
    if (DECISION == "HOURLY"):
        plt.xlabel('Observation Time (24H)') # label X axis
        plt.xticks(range(0, 24, 6))  # ticks from 0 to 23 every 6
        plt.title(f'Hourly PM2.5 Concentration in {site_to_plot} (24H), 2024 vs. 2025')

    if (DECISION == "MONTHLY"):
        plt.xlabel('Month of Observation') # label X axis
        plt.xticks(range(1, 13), ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'])  # ticks every month
        plt.title(f'Monthly PM2.5 Concentration in {site_to_plot}, 2024 vs. 2025')

    plt.ylabel('PM2.5 Concentration, µg/m3') # label Y axis
    plt.legend() # show legend
    plt.ylim(top=15)  # adjust the top leaving bottom unchanged
    plt.ylim(bottom=2.6666)  # adjust the bottom leaving top unchanged

    #GRID
    plt.grid(True) # enable grid

    #SHOW PLOT
    plt.tight_layout()  # adjusts layout so labels don't overlap
    plt.show()


elif (SITE_DECISION == "ITERATE"): 

    for location in site_id_list: #plot all the locations in the dataset

        site_to_plot = location[1]

    # for site_to_plot in ['broadway_and_35th', 'ccny', 'fdr', 'manhattan_bridge', 'midtown_west', 'queensboro_bridge', 'wheights_hamilton_bridge', 'wheights_washington_bridge', 'williamsburg_bridge']: 

        for year in list_of_years: #iterate through the years, now we are plotting the data

            if (DECISION == "HOURLY"):

                df_to_plot = list_of_grouped_hourly_dfs[year][list_of_grouped_hourly_dfs[year]['SiteID'] == site_to_plot].copy() #copy the original data to not damage it, by site
                # NOTE: WE DO NOT REMOVE "INVALID MONTHS" BECAUSE THE CONCEPT DOES NOT APPLY HERE-- WE CAN REMOVE CHUNKS OF NON PRESENT DATA, BUT WHEN IT GETS AGGREGATED INTO THE HOURLIES IT DOESN'T MAKE A DIFFERENCE

                #PLOT HOURLY
                df_to_plot['Hour'] = df_to_plot['ObservationTimeET'].apply(lambda t: t.hour + t.minute/60 + t.second/3600) # convert the pandas time unit to a plt-readable hour unit
                plt.plot(df_to_plot['Hour'], df_to_plot['Value'], label=f'{year}', marker='o', linestyle='-') #plot hourly against pm2.5 concentration with a legend for each year, and lines and points
                

            if (DECISION == "MONTHLY"):
                #generate list of valid months out of the year for the site
                valid_months_for_year = valid_check_df[ 
                (valid_check_df['Site'] == site_to_plot) & #the site is right
                (valid_check_df['YearMonth'].str.startswith(year)) & #the year is right
                (valid_check_df['Valid?'] == True) #the data is valid
                ]['YearMonth'].apply(lambda x: int(x.split('-')[1])) #separate months from years

                valid_months_for_year = valid_months_for_year.tolist()
                # list_of_grouped_monthly_dfs[year] = list_of_grouped_monthly_dfs[year][list_of_grouped_monthly_dfs[year]['Month'].isin(valid_months_for_year)] #filter out the invalid months out of the year for the site

                df_to_plot = list_of_grouped_monthly_dfs[year][list_of_grouped_monthly_dfs[year]['SiteID'] == site_to_plot].copy() #copy the original data to not damage it, by site

                df_to_plot.loc[~df_to_plot['Month'].isin(valid_months_for_year), 'Value'] = pd.NA #make "NaN" the value for invalid months

                df_to_plot['Month/Year'] = df_to_plot['Month'].astype(str) + "/" + year #prepare the list for export of summary

                summary_data_df_all_years = pd.concat([summary_data_df_all_years, df_to_plot], ignore_index=False) #add list to export summary df

                #PLOT MONTHLY
                plt.plot(df_to_plot['Month'], df_to_plot['Value'], label=f'{year}', marker='o', linestyle='-') #plot monthly against pm2.5 concentration with a legend for each year, and lines and points

        #LABELS & TITLE

        if (DECISION == "HOURLY"):
            plt.xlabel('Observation Time (24H)') # label X axis
            plt.xticks(range(0, 24, 6))  # ticks from 0 to 23 every 6
            plt.title(f'Hourly PM2.5 Concentration in {site_to_plot} (24H), 2022 through 2025')

        if (DECISION == "MONTHLY"):
            plt.xlabel('Month of Observation') # label X axis
            plt.xticks(range(1, 13), ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'])  # ticks every month
            plt.title(f'Monthly PM2.5 Concentration in {site_to_plot}, 2022 through 2025')

        plt.ylabel('PM2.5 Concentration, µg/m3') # label Y axis
        plt.legend() # show legend
        # plt.ylim(top=50)  # adjust the top leaving bottom unchanged, default = 15
        plt.ylim(bottom=2.6666)  # adjust the bottom leaving top unchanged

        #GRID
        plt.grid(True) # enable grid

        #SHOW PLOT
        plt.tight_layout()  # adjusts layout so labels don't overlap
        plt.show()

summary_data_df_all_years = summary_data_df_all_years.pivot(
    index = "SiteID",                         #rows: the sites
    columns = "Month/Year",                   #columns: the months
    values = "Value"                          #cell values: pm2.5 conc
)

#reorder summary df to match site_coords sheet
summary_data_df_all_years = summary_data_df_all_years.reindex(order) 

#convert "1/2024" → datetime object
summary_data_df_all_years.columns = pd.to_datetime(summary_data_df_all_years.columns, format="%m/%Y")

#sort chronologically
summary_data_df_all_years = summary_data_df_all_years.sort_index(axis=1)

#convert back to original labels (optional)
#summary_data_df_all_years.columns = summary_data_df_all_years.columns.strftime("%-m/%Y")   # on Linux/Mac
summary_data_df_all_years.columns = summary_data_df_all_years.columns.strftime("%#m/%Y") # on Windows (use this one)

# print(summary_data_df_all_years)

#export to excel
summary_data_df_all_years.to_excel("doh_monthly_summary_data_2022-2025.xlsx")
