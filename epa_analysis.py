import pandas as pd #table reading library: Pandas
from pathlib import Path # we use Path to establish a location for the folder where our collection of monthly data files lives
from matplotlib import pyplot as plt #plotting library to graph info visually
from enum import Enum #make enumerated keywords (words with numerical values)

class AQI(Enum):
    VALID_DATA_CUTOFF = 0.6  #defined as the minimum proportion of data retained for the month to be considered "lossless" 

SITE_SELECTION_STYLE = ("LIST", "ITERATE") #list of possible ways to select sites in hourly analysis
SITE_DECISION = SITE_SELECTION_STYLE[0] #deciaion index

site_id_list = ['Fort Lee Near Road', 'Paterson', 'Union City High School', 'Jersey City Firehouse', 'Elizabeth Lab', 'RICHMOND POST OFFICE'] #sites of interest

order = [
    "IS 45",
    "Rahway",
    "Rockland County",
    "Elizabeth Lab",
    "Queens College Near Road",
    "BABYLON",
    "NEWBURGH",
    "Chester",
    "Paterson",
    "Jersey City Firehouse",
    "WHITE PLAINS",
    "RICHMOND POST OFFICE",
    "EISENHOWER PARK",
    "Rutgers University",
    "Toms River",
    "Flemington",
    "Fort Lee Near Road",
    "PS 19",
    "PFIZER LAB SITE",
    "HOLTSVILLE",
    "Union City High School",
    "JHS 126",
] # order of summary df to match site_coords sheet


#TEMPLATE TO FILL IN VARIABLE NAMES BY YEAR
list_of_years = ["2024", "2025"] #EDIT HERE TO UPDATE THE LIST OF YEARS TO CHECK
list_of_source_files = {} #source file dictionary, keeps track of source files through the years
list_of_dfs = {} #df dictionary, keeps track of dfs through the years
list_of_grouped_hourly_dfs = {} #same as above, but grouped by hour
list_of_grouped_monthly_dfs = {} #same as above, but grouped by month

missing_check_summary_data = [] #sheet that shows amount of data available per site per month

summary_data_df_all_years = pd.DataFrame() #df of summary of pm2.5 data in the following format:
# SITE NAME         1-2024          2-2024          3-2024             4-2024           5-2024 .. ETC               PM2.5 
# ,,                    ,,              ,,              ,,                  ,,              ,,                         ,,

for year in list_of_years: #iterate through the years to create the variable names

    globals()[f"source_files_{year}"] = Path(f'nyc_epa_pm2.5_{year}.csv') #ex: nyc_epa_pm2.5_2024.csv 
    list_of_source_files[year] = globals()[f"source_files_{year}"]

    globals()[f"df_{year}"] = pd.DataFrame()
    list_of_dfs[year] = globals()[f"df_{year}"]

for year in list_of_years: #iterate through the years
    
    list_of_dfs[year] = pd.read_csv(list_of_source_files[year], header=0, sep=",") # read the yearly file, acknowledge first-line header, acknolwedge comma separation
    
    names = list_of_dfs[year]["Local Site Name"].unique().tolist() #create a list of unique names of air quality sites 

    list_of_dfs[year]["Date"] = pd.to_datetime(list_of_dfs[year]["Date"]) #convert date to pd-readable format
    list_of_dfs[year]["Month"] = list_of_dfs[year]["Date"].dt.month #add new column for month

    # list_of_dfs[year] = list_of_dfs[year].drop_duplicates(subset=["Local Site Name", "Date"]) #DROP DATA DUPES AT THE SAME SITE ON THE SAME DAY, 
                                                                                              #CONSIDER REWORKING THIS WHEN STANDARDS ARE CLEARER AS TO WHICH METHODS TO PREFER
                                                                    
    list_of_dfs[year] = list_of_dfs[year][list_of_dfs[year]['Local Site Name'].isin(order)] #DON'T DROP ANY EPA SITE NAMES
    
    list_of_dfs[year] = list_of_dfs[year][(list_of_dfs[year]['Method Description'].isin(['Met One BAM-1022 Mass Monitor w/ VSCC or TE-PM2.5C', 'PM2.5 SCC w/Correction Factor'])) | (list_of_dfs[year]['Source'] == 'AirNow')] #TAKE DATA WITH ONLY ONE METHOD, AND USE AIRNOW AS WELL

    # list_of_dfs[year] = list_of_dfs[year][(list_of_dfs[year]['Method Description'] == 'Met One BAM-1022 Mass Monitor w/ VSCC or TE-PM2.5C')        | (list_of_dfs[year]['Source'] == 'AirNow')] #TAKE DATA WITH ONLY ONE METHOD, AND USE AIRNOW AS WELL
    # list_of_dfs[year] = list_of_dfs[year][(list_of_dfs[year]['Method Description'] == 'PM2.5 SCC w/Correction Factor')                             | (list_of_dfs[year]['Source'] == 'AirNow')] #TAKE DATA WITH ONLY ONE METHOD, AND USE AIRNOW AS WELL
    # list_of_dfs[year] = list_of_dfs[year][(list_of_dfs[year]['Method Description'] == 'R & P Model 2025 PM-2.5 Sequential Air Sampler w/VSCC')     | (list_of_dfs[year]['Source'] == 'AirNow')] #TAKE DATA WITH ONLY ONE METHOD, AND USE AIRNOW AS WELL
    # list_of_dfs[year] = list_of_dfs[year][(list_of_dfs[year]['Method Description'] == 'Thermo Scientific 5014i or FH62C14-DHS w/VSCC')             | (list_of_dfs[year]['Source'] == 'AirNow')] #TAKE DATA WITH ONLY ONE METHOD, AND USE AIRNOW AS WELL
    # list_of_dfs[year] = list_of_dfs[year][(list_of_dfs[year]['Method Description'] == 'Teledyne T640 at 5.0 LPM w/Network Data Alignment enabled') | (list_of_dfs[year]['Source'] == 'AirNow')] #TAKE DATA WITH ONLY ONE METHOD, AND USE AIRNOW AS WELL
    
    # list_of_dfs[year] = list_of_dfs[year][(list_of_dfs[year]['Method Description'] == 'Met One BAM-1022 Mass Monitor w/ VSCC or TE-PM2.5C')                                                   ] #TAKE DATA WITH ONLY ONE METHOD
    # list_of_dfs[year] = list_of_dfs[year][(list_of_dfs[year]['Method Description'] == 'PM2.5 SCC w/Correction Factor')                                                                        ] #TAKE DATA WITH ONLY ONE METHOD, AND USE AIRNOW AS WELL
    # list_of_dfs[year] = list_of_dfs[year][(list_of_dfs[year]['Method Description'] == 'R & P Model 2025 PM-2.5 Sequential Air Sampler w/VSCC')                                                ] #TAKE DATA WITH ONLY ONE METHOD, AND USE AIRNOW AS WELL
    # list_of_dfs[year] = list_of_dfs[year][(list_of_dfs[year]['Method Description'] == 'Thermo Scientific 5014i or FH62C14-DHS w/VSCC')                                                        ] #TAKE DATA WITH ONLY ONE METHOD, AND USE AIRNOW AS WELL
    # list_of_dfs[year] = list_of_dfs[year][(list_of_dfs[year]['Method Description'] == 'Teledyne T640 at 5.0 LPM w/Network Data Alignment enabled')                                            ] #TAKE DATA WITH ONLY ONE METHOD, AND USE AIRNOW AS WELL

    for month in range(1, 13, 1): #iterate through months of year (1-12, jan-dec)
        for site in order: #iterate through each EPA site name

            valid_count = len(list_of_dfs[year][
                    (list_of_dfs[year]["Local Site Name"].isin([site])) &
                    (list_of_dfs[year]["Month"] == month)
                ]
            ) #count the number of entries by site, and by month

            missing_check_summary_data.append({ # add to the valid data tracking sheet

                "YearMonth": str(month) + "/" + str(year),
                "Site": site,
                "ValidCount": valid_count,
                "Cutoff": AQI.VALID_DATA_CUTOFF.value * 30,
                "Valid?": valid_count >= AQI.VALID_DATA_CUTOFF.value * 30,
                "ValidPct": round(valid_count / 31 * 100, 2)

            })

#convert summary data into a DataFrame
valid_check_df = pd.DataFrame(missing_check_summary_data)
valid_check_df = valid_check_df[valid_check_df['Site'].isin(order)] #DROP NON EPA SITE NAMES

#pivot so each month is a row (Y-axis), each site is a column (X-axis)
pivot_valid_check_df = valid_check_df.pivot_table(index="YearMonth", columns="Site", values="ValidPct", fill_value=0)

pivot_valid_check_df = pivot_valid_check_df.reset_index() # temporarily release index hold from pivot to work with YearMonth column
pivot_valid_check_df["YearMonth_dt"] = pd.to_datetime(pivot_valid_check_df["YearMonth"], format="%m/%Y") #make a helper column for the validity chart month's name, pd-readable
pivot_valid_check_df = pivot_valid_check_df.sort_values("YearMonth_dt") #sort by month on helper
pivot_valid_check_df = pivot_valid_check_df.drop(columns="YearMonth_dt") #drop helper
pivot_valid_check_df = pivot_valid_check_df.set_index("YearMonth")  #restore original index

#export to excel
# pivot_valid_check_df.to_excel("epa_monthly_validity_summ2ry.xlsx")


#get coords and siteIDs for all EPA sites

# df_all = pd.DataFrame()  # empty DataFrame
# for year in list_of_years:
#     df_all = pd.concat([df_all, list_of_dfs[year]], ignore_index=True) #combine all entries into one df

# site_coords_list = df_all[["Local Site Name", "Site ID", "Site Latitude", "Site Longitude"]].values.tolist() #take site name and coords from all entries

# site_coords_list = [list(x) for x in set(tuple(x) for x in site_coords_list)] #remove duplicates in list of coordinates

# site_list_df = pd.DataFrame(site_coords_list, columns=["Local Site Name", "Site ID", "Latitude", "Longitude"]) #convert list to df
# site_list_df.to_excel("site_coords.xlsx", index=False) #export to excel

for year in list_of_years: #iterate through the years

    #MONTHLY GROUP
    globals()[f"grouped_monthly_df_{year}"] = list_of_dfs[year].groupby(['Local Site Name', 'Month'], as_index=False)['Daily Mean PM2.5 Concentration'].mean() #make a dataset that groups all the site data together, splitting by month
    list_of_grouped_monthly_dfs[year] = globals()[f"grouped_monthly_df_{year}"]

    # print(list_of_grouped_monthly_dfs[year])


if (SITE_DECISION == "LIST"):

    site_to_plot = ("Jersey City Firehouse", "Union City High School")
    # site_to_plot = site_id_list

    for year in list_of_years:

        df_list = []

        for site in site_to_plot:

            # select data for this site
            df_site = list_of_grouped_monthly_dfs[year][list_of_grouped_monthly_dfs[year]['Local Site Name'] == site].copy()

            # find valid months for this site and year
            valid_months_for_site = valid_check_df[
                (valid_check_df['Site'] == site) &
                (valid_check_df['YearMonth'].str.endswith(year)) &
                (valid_check_df['Valid?'] == True)
            ]['YearMonth'].apply(lambda x: int(x.split('/')[0])).tolist()

            # keep only valid months
            df_site = df_site[df_site['Month'].isin(valid_months_for_site)]

            df_list.append(df_site)

        # combine all sites
        df_to_plot = pd.concat(df_list)

        # average across sites for each month
        df_to_plot = df_to_plot.groupby('Month', as_index=False)['Daily Mean PM2.5 Concentration'].mean()

        # PLOT MONTHLY
        plt.plot(df_to_plot['Month'], df_to_plot['Daily Mean PM2.5 Concentration'], label=f'{year}', marker='o', linestyle='-')


    plt.xlabel('Month of Observation') # label X axis
    plt.xticks(range(1, 13), ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'])  # ticks every month
    plt.title(f'Monthly PM2.5 Concentration in {site_to_plot}, 2024 vs. 2025')

    plt.ylabel('PM2.5 Concentration, µg/m3') # label Y axis
    plt.ylim(top=15)  # adjust the top leaving bottom unchanged
    plt.ylim(bottom=2.6666)  # adjust the bottom leaving top unchanged
    plt.legend() # show legend

    #GRID
    plt.grid(True) # enable grid

    #SHOW PLOT
    plt.tight_layout()  # adjusts layout so labels don't overlap
    plt.show()


elif (SITE_DECISION == "ITERATE"): 

    # for location in site_id_list: #plot all the locations in the sites of interest
    for location in order: #plot all the locations in the EPA dataset

        site_to_plot = location

    # for site_to_plot in ['cross_bronx_expwy', 'broadway_and_35th', 'mott_haven']: 

        for year in list_of_years: #iterate through the years, now we are plotting the data                

            #generate list of valid months out of the year for the site
            valid_months_for_year = valid_check_df[ 
            (valid_check_df['Site'] == site_to_plot) & #the site is right
            (valid_check_df['YearMonth'].str.endswith(year)) & #the year is right
            (valid_check_df['Valid?'] == True) #the data is valid
            ]['YearMonth'].apply(lambda x: int(x.split('/')[0])) #separate months from years

            valid_months_for_year = valid_months_for_year.tolist()

            # list_of_grouped_monthly_dfs[year] = list_of_grouped_monthly_dfs[year][list_of_grouped_monthly_dfs[year]['Month'].isin(valid_months_for_year)] #filter out the invalid months out of the year for the site


            df_to_plot = list_of_grouped_monthly_dfs[year][list_of_grouped_monthly_dfs[year]['Local Site Name'] == site_to_plot].copy() #copy the original data to not damage it, by site

            df_to_plot.loc[~df_to_plot['Month'].isin(valid_months_for_year), 'Daily Mean PM2.5 Concentration'] = pd.NA #make "NaN" the value for invalid months
    
            df_to_plot['Month/Year'] = df_to_plot['Month'].astype(str) + "/" + year #prepare the list for export of summary

            summary_data_df_all_years = pd.concat([summary_data_df_all_years, df_to_plot], ignore_index=False) #add list to export summary df
            
            #PLOT MONTHLY
            plt.plot(df_to_plot['Month'], df_to_plot['Daily Mean PM2.5 Concentration'], label=f'{year}', marker='o', linestyle='-') #plot monthly against pm2.5 concentration with a legend for each year, and lines and points



        #LABELS & TITLE
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

summary_data_df_all_years = summary_data_df_all_years.pivot(
    index = "Local Site Name",                #rows: the sites
    columns = "Month/Year",                   #columns: the months
    values = "Daily Mean PM2.5 Concentration" #cell values
)

#reorder summary df to match site_coords sheet
summary_data_df_all_years = summary_data_df_all_years.reindex(order) 

#convert "1/2024" → datetime object
summary_data_df_all_years.columns = pd.to_datetime(summary_data_df_all_years.columns, format="%m/%Y")

#sort chronologically
summary_data_df_all_years = summary_data_df_all_years.sort_index(axis=1)

#convert back to your original labels (optional)
#summary_data_df_all_years.columns = summary_data_df_all_years.columns.strftime("%-m/%Y")   # on Linux/Mac
summary_data_df_all_years.columns = summary_data_df_all_years.columns.strftime("%#m/%Y") # on Windows (use this one)

# print(summary_data_df_all_years)

#export to excel
# summary_data_df_all_years.to_excel("epa_monthly_summary_data.xlsx")

