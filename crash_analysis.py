import pandas as pd #table reading library: Pandas
from pathlib import Path # we use Path to establish a location for the folder where our collection of monthly data files lives
from matplotlib import pyplot as plt #plotting library to graph info visually
from enum import Enum #make enumerated keywords (words with numerical values)
import duckdb #for processing the sql query and applying it to persons crash sheet
import geopandas as gpd #for putting coordinate-ready person-crash data in neighborhoods w/ NTA/CRZ shapefiles
from shapely.ops import unary_union #Shapely is a geometric analysis tool. we want to be able to union different polygons together with unary_union
from shapely.geometry import Point  #                                       use Point to turn crash locations into points

import numpy as np #to select from our list of possible KABCU categories
list_of_severities = ["K", "A", "B", "C"] #list of severities we want to see data on, change as needed

pcn_path = Path("persons_and_crashes_and_neighborhoods.csv")
if not(pcn_path.exists()): #if we don't already have the processed persons/crashes/neighborhoods datasheet, 
                           #ONLY THEN do we create it (avoids duplicate creation (current time: 2m 14.235s, avoid if not needed))

    crashes_df = pd.read_csv('crashes.csv', dtype={'COLLISION_ID': 'string'}) #read crash dataset
    person_df = pd.read_csv('person.csv', dtype={'COLLISION_ID': 'string'}) #read persons dataset


    joined_df = person_df.merge(crashes_df, on='COLLISION_ID', how='inner') #merge persons -> crashes (column order) by COLLISION_ID

    #SQL QUERY FROM NYCDOT VISION ZERO RESEARCH TEAM, TRANSLATED INTO PYTHON (courtesy: Rob Viola)
    # new_col = duckdb.query_df(joined_df, "joined_sql_view", """

    #     SELECT
    #         CASE
    #             when PERSON_INJURY = 'Killed' then 'K'
    #             when PERSON_INJURY = 'Injured' and EMOTIONAL_STATUS in ('Unconscious','Semiconscious','Incoherent') then 'A'
    #             when PERSON_INJURY = 'Injured' and coalesce(EMOTIONAL_STATUS, 'Unknown') in ('Apparent Death', 'Shock', 'Conscious', 'Unknown','Does Not Apply') and COMPLAINT in
    #             ('Amputation','Concussion','Internal','Severe Bleeding','Moderate Burn'
    #             ,'Severe Burn','Fracture - Dislocation', 'Fracture - Distorted - Dislocation',
    #             'Crush Injuries','Paralysis','Severe Lacerations') then 'A'
    #             when PERSON_INJURY = 'Injured' and coalesce(EMOTIONAL_STATUS, 'Unknown') in ('Apparent Death', 'Shock', 'Conscious', 'Unknown','Does Not Apply') and COMPLAINT in
    #             ('Minor Bleeding','Minor Burn','Complaint of Pain','Complaint of Pain or Nausea') then 'B'
    #             when PERSON_INJURY = 'Injured' and coalesce(EMOTIONAL_STATUS, 'Unknown') in ('Apparent Death', 'Shock', 'Conscious', 'Unknown','Does Not Apply')
    #             and COMPLAINT in ('Contusion - Bruise','Abrasion') then 'B'
    #             when PERSON_INJURY = 'Injured' and coalesce(EMOTIONAL_STATUS, 'Unknown') in ('Apparent Death', 'Shock', 'Conscious', 'Unknown','Does Not Apply')
    #             and COMPLAINT in ('Minor Bleeding','Minor Burn')
    #             and coalesce(BODILY_INJURY,'Unknown') != 'Eye' then 'B'
    #             when PERSON_INJURY = 'Injured' and coalesce(EMOTIONAL_STATUS, 'Unknown') in ('Apparent Death', 'Shock', 'Conscious', 'Unknown','Does Not Apply')
    #             and COMPLAINT in ('Complaint of Pain', 'Complaint of Pain or Nausea')
    #             and coalesce(BODILY_INJURY,'Unknown') != 'Eye' then 'C'
    #             when PERSON_INJURY = 'Injured' and EMOTIONAL_STATUS = 'Conscious' and coalesce(COMPLAINT, 'Unknown') in ('Unknown', 'None Visible', 'Does Not Apply')
    #             and coalesce(BODILY_INJURY, 'Unknown') in ('Unknown', 'Does Not Apply') then 'U'
    #             when PERSON_INJURY = 'Injured' and coalesce(EMOTIONAL_STATUS, 'Unknown') in ('Apparent Death', 'Shock', 'Conscious', 'Unknown','Does Not Apply')
    #             and coalesce(COMPLAINT, 'Unknown') in ('Unknown', 'None Visible', 'Whiplash', 'Does Not Apply') then 'C'           
    #             when PERSON_INJURY = 'Injured' and EMOTIONAL_STATUS = 'Shock' and coalesce(COMPLAINT, 'Unknown') in ('Unknown', 'None Visible', 'Whiplash', 'Does Not Apply')
    #             and coalesce(BODILY_INJURY, 'Unknown') in ('Unknown', 'Does Not Apply', 'Whiplash') then 'C'
    #             else 'U' 
    #         END as SEVERITY
    #     FROM joined_sql_view

    # """).to_df()

    # joined_df["SEVERITY"] = new_col["SEVERITY"]


    # Make sure string columns are clean
    joined_df['PERSON_INJURY'] = joined_df['PERSON_INJURY'].astype(str).str.strip().str.upper()
    joined_df['EMOTIONAL_STATUS'] = joined_df['EMOTIONAL_STATUS'].astype(str).str.strip().str.upper()
    joined_df['COMPLAINT'] = joined_df['COMPLAINT'].astype(str).str.strip().str.upper()
    joined_df['BODILY_INJURY'] = joined_df['BODILY_INJURY'].astype(str).str.strip().str.upper()

    # Define conditions
    conditions = [
        # Killed = K
        joined_df['PERSON_INJURY'] == 'KILLED',

        # Injured with serious emotional status = A
        (joined_df['PERSON_INJURY'] == 'INJURED') & (joined_df['EMOTIONAL_STATUS'].isin(['UNCONSCIOUS','SEMICONSCIOUS','INCOHERENT'])),

        # Injured with apparent death/shock/conscious + severe complaints = A
        (joined_df['PERSON_INJURY'] == 'INJURED') &
        (joined_df['EMOTIONAL_STATUS'].isin(['APPARENT DEATH','SHOCK','CONSCIOUS','UNKNOWN','DOES NOT APPLY'])) &
        (joined_df['COMPLAINT'].isin([
            'AMPUTATION','CONCUSSION','INTERNAL','SEVERE BLEEDING','MODERATE BURN',
            'SEVERE BURN','FRACTURE - DISLOCATION','FRACTURE - DISTORTED - DISLOCATION',
            'CRUSH INJURIES','PARALYSIS','SEVERE LACERATIONS'
        ])),

        # Injured with minor complaints = B
        (joined_df['PERSON_INJURY'] == 'INJURED') &
        (joined_df['EMOTIONAL_STATUS'].isin(['APPARENT DEATH','SHOCK','CONSCIOUS','UNKNOWN','DOES NOT APPLY'])) &
        (joined_df['COMPLAINT'].isin(['MINOR BLEEDING','MINOR BURN','COMPLAINT OF PAIN','COMPLAINT OF PAIN OR NAUSEA'])),

        # Injured with contusions/abrasions = B
        (joined_df['PERSON_INJURY'] == 'INJURED') &
        (joined_df['EMOTIONAL_STATUS'].isin(['APPARENT DEATH','SHOCK','CONSCIOUS','UNKNOWN','DOES NOT APPLY'])) &
        (joined_df['COMPLAINT'].isin(['CONTUSION - BRUISE','ABRASION'])),

        # Injured minor + BODILY_INJURY not Eye = B
        (joined_df['PERSON_INJURY'] == 'INJURED') &
        (joined_df['EMOTIONAL_STATUS'].isin(['APPARENT DEATH','SHOCK','CONSCIOUS','UNKNOWN','DOES NOT APPLY'])) &
        (joined_df['COMPLAINT'].isin(['MINOR BLEEDING','MINOR BURN'])) &
        (joined_df['BODILY_INJURY'] != 'EYE'),

        # Injured pain complaints not Eye = C
        (joined_df['PERSON_INJURY'] == 'INJURED') &
        (joined_df['EMOTIONAL_STATUS'].isin(['APPARENT DEATH','SHOCK','CONSCIOUS','UNKNOWN','DOES NOT APPLY'])) &
        (joined_df['COMPLAINT'].isin(['COMPLAINT OF PAIN','COMPLAINT OF PAIN OR NAUSEA'])) &
        (joined_df['BODILY_INJURY'] != 'EYE'),

        # Conscious with unknown/no visible injury = U
        (joined_df['PERSON_INJURY'] == 'INJURED') &
        (joined_df['EMOTIONAL_STATUS'] == 'CONSCIOUS') &
        (joined_df['COMPLAINT'].isin(['UNKNOWN','NONE VISIBLE','DOES NOT APPLY'])) &
        (joined_df['BODILY_INJURY'].isin(['UNKNOWN','DOES NOT APPLY'])),

        # Other injured cases that become C
        (joined_df['PERSON_INJURY'] == 'INJURED') &
        (joined_df['EMOTIONAL_STATUS'].isin(['APPARENT DEATH','SHOCK','CONSCIOUS','UNKNOWN','DOES NOT APPLY'])) &
        (joined_df['COMPLAINT'].isin(['UNKNOWN','NONE VISIBLE','WHIPLASH','DOES NOT APPLY'])),

        (joined_df['PERSON_INJURY'] == 'INJURED') &
        (joined_df['EMOTIONAL_STATUS'] == 'SHOCK') &
        (joined_df['COMPLAINT'].isin(['UNKNOWN','NONE VISIBLE','WHIPLASH','DOES NOT APPLY'])) &
        (joined_df['BODILY_INJURY'].isin(['UNKNOWN','DOES NOT APPLY','WHIPLASH']))
    ]

    # Corresponding values
    choices = ['K','A','A','B','B','B','C','U','C','C']

    # Apply the logic
    joined_df['SEVERITY'] = np.select(conditions, choices, default='U')

    #\SQL QUERY FROM NYCDOT VISION ZERO RESEARCH TEAM, TRANSLATED INTO PYTHON (courtesy: Rob Viola)

    # joined_df.to_csv("persons_and_crashes.csv", index=False)


    #WORK WITH NTA SHAPEFILE

    #read shapefile
    gdf = gpd.read_file("NTAs/geo_export_bd71e9cd-14bd-435b-99c8-b573a94a1dc8.shp")

    # gdf.to_csv("ntas.csv", index=False)

    #list of NTAs in the CRZ
    crz_ntas = [
    "Financial District-Battery Park City",
    "Tribeca-Civic Center",
    "The Battery-Governors Island-Ellis Island-Liberty Island",
    "SoHo-Little Italy-Hudson Square",
    "Greenwich Village",
    "West Village",
    "Chinatown-Two Bridges",
    "Lower East Side",
    "East Village",
    "Chelsea-Hudson Yards",
    "Hell's Kitchen",
    "Midtown South-Flatiron-Union Square",
    "Midtown-Times Square",
    "Stuyvesant Town-Peter Cooper Village",
    "Gramercy",
    "Murray Hill-Kips Bay",
    "East Midtown-Turtle Bay",
    "United Nations"]

    #new gdf consisting solely of NTAs in CRZ
    crz_nta_gdf = gdf[gdf["ntaname"].isin(crz_ntas)]

    #new gdf consisting solely of NTAs in UWS
    uws_nta_gdf = gdf[gdf["cdtaname"].str.contains("MN07")]

    #new gdf consisting solely of NTAs in UES
    ues_nta_gdf = gdf[gdf["cdtaname"].str.contains("MN08")]

    #new gdf consisting solely of NTAs in Williamsburg
    wlb_nta_gdf = gdf[gdf["ntaname"].str.contains("Williamsburg")]

    #new gdf consisting solely of NTAs in Downtown Brooklyn (incl. ft greene, navy yard, clinton hill)
    dtb_nta_gdf = gdf[gdf["cdtaname"].str.contains("BK02")]

    #new gdf consisting solely of NTAs in Astoria (incl. st.mchl cem, excl. rikers)
    ast_nta_gdf = gdf[gdf["cdtaname"].str.contains("QN01") & gdf["boroname"].str.contains("Queens")]

    #new gdf consisting solely of NTAs in Harlem (incl. west/east, excl. Morningside)
    hlm_nta_gdf = gdf[(gdf["cdtaname"].str.contains("MN09") | gdf["cdtaname"].str.contains("MN10") | gdf["cdtaname"].str.contains("MN11")) & ~gdf["ntaname"].str.contains("Morningside")]

    #new gdf consisting solely of NTAs In The Heights! / Inwood
    upm_nta_gdf = gdf[gdf["cdtaname"].str.contains("MN12")]


    #join the NTAs in CRZ together
    crz_nta_gdf_combined_geom = unary_union(crz_nta_gdf.geometry)

    #join the NTAs in UWS together
    uws_nta_gdf_combined_geom = unary_union(uws_nta_gdf.geometry)

    #join the NTAs in UES together
    ues_nta_gdf_combined_geom = unary_union(ues_nta_gdf.geometry)

    #join the NTAs in WLB together
    wlb_nta_gdf_combined_geom = unary_union(wlb_nta_gdf.geometry)

    #join the NTAs in DTB together
    dtb_nta_gdf_combined_geom = unary_union(dtb_nta_gdf.geometry)

    #join the NTAs in AST together
    ast_nta_gdf_combined_geom = unary_union(ast_nta_gdf.geometry)

    #join the NTAs in HLM together
    hlm_nta_gdf_combined_geom = unary_union(hlm_nta_gdf.geometry)

    #join the NTAs in UPM together
    upm_nta_gdf_combined_geom = unary_union(upm_nta_gdf.geometry)


    #make a new gdf that is the combined geometry of CRZ NTAs
    crz_nta_combined_gdf = gpd.GeoDataFrame([{
        "ntaname": "Congestion Relief Zone",
        "geometry": crz_nta_gdf_combined_geom
    }], crs=gdf.crs) #new coordinate reference system = old coordinate reference system of original gdf

    #make a new gdf that is the combined geometry of UWS NTAs
    uws_nta_combined_gdf = gpd.GeoDataFrame([{
        "ntaname": "Upper West Side",
        "geometry": uws_nta_gdf_combined_geom
    }], crs=gdf.crs) #new coordinate reference system = old coordinate reference system of original gdf

    #make a new gdf that is the combined geometry of UES NTAs
    ues_nta_combined_gdf = gpd.GeoDataFrame([{
        "ntaname": "Upper East Side",
        "geometry": ues_nta_gdf_combined_geom
    }], crs=gdf.crs) #new coordinate reference system = old coordinate reference system of original gdf

    #make a new gdf that is the combined geometry of WLB NTAs
    wlb_nta_combined_gdf = gpd.GeoDataFrame([{
        "ntaname": "Williamsburg",
        "geometry": wlb_nta_gdf_combined_geom
    }], crs=gdf.crs) #new coordinate reference system = old coordinate reference system of original gdf

    #make a new gdf that is the combined geometry of DTB NTAs
    dtb_nta_combined_gdf = gpd.GeoDataFrame([{
        "ntaname": "Downtown Brooklyn",
        "geometry": dtb_nta_gdf_combined_geom
    }], crs=gdf.crs) #new coordinate reference system = old coordinate reference system of original gdf

    #make a new gdf that is the combined geometry of AST NTAs
    ast_nta_combined_gdf = gpd.GeoDataFrame([{
        "ntaname": "Astoria",
        "geometry": ast_nta_gdf_combined_geom
    }], crs=gdf.crs) #new coordinate reference system = old coordinate reference system of original gdf

    #make a new gdf that is the combined geometry of HLM NTAs
    hlm_nta_combined_gdf = gpd.GeoDataFrame([{
        "ntaname": "Greater Harlem",
        "geometry": hlm_nta_gdf_combined_geom
    }], crs=gdf.crs) #new coordinate reference system = old coordinate reference system of original gdf

    #make a new gdf that is the combined geometry of UPM NTAs
    upm_nta_combined_gdf = gpd.GeoDataFrame([{
        "ntaname": "Inwood/Washington Heights",
        "geometry": upm_nta_gdf_combined_geom
    }], crs=gdf.crs) #new coordinate reference system = old coordinate reference system of original gdf

    #make a new gdf that is NOT the other groups of NTAs
    rest_gdf = gdf[
        ~gdf["ntaname"].isin(crz_ntas) &                              #CRZ
        ~gdf["cdtaname"].str.contains("MN07", na=False) &            #UWS
        ~gdf["cdtaname"].str.contains("MN08", na=False) &            #UES
        ~gdf["ntaname"].str.contains("Williamsburg", na=False) &     #Williamsburg
        ~gdf["cdtaname"].str.contains("BK02", na=False) &            #Downtown Brooklyn
        ~(gdf["cdtaname"].str.contains("QN01", na=False) & gdf["boroname"].str.contains("Queens", na=False)) &  #Astoria
        ~((gdf["cdtaname"].str.contains("MN09", na=False) |           #Harlem
        gdf["cdtaname"].str.contains("MN10", na=False) | 
        gdf["cdtaname"].str.contains("MN11", na=False)) & 
        ~gdf["ntaname"].str.contains("Morningside", na=False)) & 
        ~gdf["cdtaname"].str.contains("MN12", na=False)              #Inwood / The Heights
    ]
    #new (last) gdf that combines grouped NTAs and others
    final_gdf = gpd.GeoDataFrame(pd.concat([rest_gdf, crz_nta_combined_gdf, uws_nta_combined_gdf, ues_nta_combined_gdf, wlb_nta_combined_gdf, dtb_nta_combined_gdf, ast_nta_combined_gdf, hlm_nta_combined_gdf, upm_nta_combined_gdf], ignore_index=True), crs=gdf.crs)

    final_gdf.plot(edgecolor="black", facecolor="lightblue")


    #make a new gdf that is the crash data, with the lat/long converted to points
    crash_points_gdf = gpd.GeoDataFrame(
        joined_df,
        geometry=gpd.points_from_xy(joined_df.LONGITUDE, joined_df.LATITUDE),
        crs=gdf.crs #new coordinate reference system = old coordinate reference system of original gdf
    )

    #join the NTA data to the crash data
    spatial_crash_points_gdf = gpd.sjoin(crash_points_gdf, final_gdf, how="left", predicate="intersects")

    # print(spatial_crash_points_gdf)

    spatial_crash_points_gdf.to_csv("persons_and_crashes_and_neighborhoods.csv", index=False)


else: #if persons/crashes/neighborhood processed dataset already exists, just focus on summarizing it

    spatial_crash_points_gdf = pd.read_csv('persons_and_crashes_and_neighborhoods.csv') #reread persons/crashes/neighborhoods dataset


#create summary sheet based on selected columns
cols_to_keep = ["CRASH_DATE", "CRASH_TIME", "SEVERITY", "PERSON_TYPE", "PERSON_AGE", "PERSON_SEX", "ntaname"]
summary_data_df = spatial_crash_points_gdf[cols_to_keep]

# summary_data_df.CRASH_DATE -> group by year
# summary_data_df.SEVERITY -> only take the K and As, group by those
# summary_data_df.ntaname -> group by individual value, allow blanks as names

#get the date formatted into a proper pd date 
summary_data_df["CRASH_DATE"] = pd.to_datetime(
    summary_data_df["CRASH_DATE"],
    format="%m/%d/%Y",   # because it's MM/DD/YYYY
    errors="coerce"
)

#make year column
summary_data_df["CRASH_YEAR"] = summary_data_df["CRASH_DATE"].dt.year

#make month column
summary_data_df["CRASH_MONTH"] = summary_data_df["CRASH_DATE"].dt.month

#omly select K/A crashes
summary_data_df = summary_data_df[summary_data_df["SEVERITY"].isin(list_of_severities)]

#group by NTA (or lack thereof), and by year / K or A
summary_data_df = (
    summary_data_df
    .groupby(["CRASH_YEAR", "CRASH_MONTH", "SEVERITY", "ntaname"], dropna=False)
    .size()
    .reset_index(name="count")
)

neighborhoods_to_keep = [ #to keep for summary needs
    "Congestion Relief Zone",
    "Upper West Side",
    "Upper East Side",
    "Williamsburg",
    "Downtown Brooklyn",
    "Astoria",
    "Greater Harlem",
    "Inwood/Washington Heights",
    "Long Island City-Hunters Point",
    "Morningside Heights"
]

#if ntaname not in keep list, change to "other"
summary_data_df["grouped_ntas"] = summary_data_df["ntaname"].where(
    summary_data_df["ntaname"].isin(neighborhoods_to_keep),
    "Other"
)

#group with new protocol
summary_data_df = (
    summary_data_df
    .groupby(["CRASH_YEAR", "CRASH_MONTH", "SEVERITY", "grouped_ntas"], as_index=False)["count"]
    .sum()
)


#build full index of all combinations of year/month/severity/grouped nta
years = summary_data_df["CRASH_YEAR"].unique()
months = summary_data_df["CRASH_MONTH"].unique()
severities = summary_data_df["SEVERITY"].unique()
groups = neighborhoods_to_keep + ["Other"]

full_index = pd.MultiIndex.from_product(
    [years, months, severities, groups],
    names=["CRASH_YEAR", "CRASH_MONTH", "SEVERITY", "grouped_ntas"]
)

#reindex to guarantee all rows exist (even if count is 0)
summary_data_df = (
    summary_data_df
    .set_index(["CRASH_YEAR", "CRASH_MONTH", "SEVERITY", "grouped_ntas"])
    .reindex(full_index, fill_value=0)
    .reset_index()
)

#sort values (not actual column positions) by severity -> year -> month -> grouped_nta
summary_data_df = summary_data_df.sort_values(
    by=["SEVERITY", "CRASH_YEAR", "CRASH_MONTH", "grouped_ntas"]
).reset_index(drop=True)


#create sum rows for non_crz adj, other, citywide, crz, while keeping non_crz_adj

#exclude crz from new adjacent list
keep_no_crz = [nta for nta in neighborhoods_to_keep if nta != "Congestion Relief Zone"]

#total for adj list
adj_total = (
    summary_data_df[summary_data_df["grouped_ntas"].isin(keep_no_crz)]
    .groupby(["CRASH_YEAR", "CRASH_MONTH", "SEVERITY"], as_index=False)["count"]
    .sum()
)
adj_total["grouped_ntas"] = "CRZ Adjacent Total"

#total for crz
crz_total = summary_data_df[summary_data_df["grouped_ntas"] == "Congestion Relief Zone"].copy()
crz_total["grouped_ntas"] = "CRZ Total"

#total for other
other_total = summary_data_df[summary_data_df["grouped_ntas"] == "Other"].copy()
other_total["grouped_ntas"] = "Other Total"

#total citywide
city_total = summary_data_df.groupby(["CRASH_YEAR", "CRASH_MONTH", "SEVERITY"], as_index=False)["count"].sum()
city_total["grouped_ntas"] = "City Total"

#select all adj neighborhoods to keep as rows
adj_rows = summary_data_df[summary_data_df["grouped_ntas"].isin(keep_no_crz)]

#combine in desired order
summary_final_df = pd.concat([
    adj_rows,
    adj_total,
    crz_total,
    other_total,
    city_total
], ignore_index=True)

#sort values (not actual column positions) by severity -> year -> month -> grouped_nta
summary_final_df = summary_final_df.sort_values(
    by=["SEVERITY", "CRASH_YEAR", "CRASH_MONTH"],
    ignore_index=True
)

summary_final_df.to_csv("persons_summary.csv", index=False)


# PLOTTING THE DATA!!
TIME_DECISION_LIST = ["Monthly", "Yearly"]
TIME_DECISION_INDEX = 1

DATA_STYLE_LIST = ["YOY", "COUNT"]
DATA_STYLE_INDEX = 0

#take the totals, we can also take indiv neighborhoods later if we want
totals = ['CRZ Total', 'CRZ Adjacent Total', 
         'Other Total', 'City Total'] # + keep_no_crz

data_totals = summary_final_df[summary_final_df['grouped_ntas'].isin(totals)]


#create a year-month index
data_totals['Year-Month'] = pd.to_datetime(data_totals['CRASH_YEAR'].astype(str) + '-' + data_totals['CRASH_MONTH'].astype(str))

#sort by year-month
data_totals = data_totals.sort_values('Year-Month')

#set size and quantity of subplots
fig, axes = plt.subplots(len(list_of_severities), 1, figsize=(14, 5*len(list_of_severities)), sharex=True)

for i, sev in enumerate(list_of_severities):

    ax = axes[i]
    
    #filter for this severity
    data_sev = data_totals[data_totals['SEVERITY'] == sev]


    if TIME_DECISION_LIST[TIME_DECISION_INDEX] == ("Monthly"): 

        #pivot
        pivot_sev = data_sev.pivot(index='Year-Month', columns='grouped_ntas', values='count')

        #calculate YOY percentage change

        #shift by 12 months (1 year) (should exclude 2019 period)
        yoy_sev = pivot_sev.pct_change(periods=12) * 100
        


    elif TIME_DECISION_LIST[TIME_DECISION_INDEX] == ("Yearly"):
        #calculate YOY percentage change based on yearly data

        #combine into yearly, exclude months as needed
        data_right_months = data_sev[data_sev["CRASH_MONTH"].between(1, 11)]

        data_yearly = (
            data_right_months
            .groupby(["CRASH_YEAR", "SEVERITY", "grouped_ntas"], as_index=False)["count"]
            .sum()
        )

        #pivot
        pivot_sev = data_yearly.pivot(index='CRASH_YEAR', columns='grouped_ntas', values='count') #<- this one is our graph

        #prep crash-by-severity data to be exported
        pivot_sev_export = data_yearly.pivot(index='grouped_ntas', columns='CRASH_YEAR', values='count')#<- this one is our to-be-exported table
        pivot_sev_export = pivot_sev_export.reindex(totals) #reorder rows to fit original order of totals indicated earlier
        pivot_sev_export = pivot_sev_export.reset_index() #reset index to show area names upon export

        #EXPORT YEARLIES TO CSV (JAN-NOV)
        pivot_sev_export.to_csv(f"crashes_{sev}_yoy_summary_2019_2025.csv", index=False)

        #make yoy_sev out of new yearly data
        yoy_sev = pivot_sev.pct_change(periods=1) * 100


    if DATA_STYLE_LIST[DATA_STYLE_INDEX] == ("YOY"):
        #plot each column (location category)
        for nta in yoy_sev.columns:
            ax.plot(yoy_sev.index, yoy_sev[nta], marker='o', label=nta)
        
        #set titles and y labels for each subplot
        ax.set_title(f"YOY % Change - {sev} Crashes")
        ax.set_ylabel("YOY % Change")

    elif DATA_STYLE_LIST[DATA_STYLE_INDEX] == ("COUNT"):
        #plot each column (location category)
        for nta in pivot_sev.columns:
            ax.plot(pivot_sev.index, pivot_sev[nta], marker='o', label=nta)
        
        #set titles and y labels for each subplot
        ax.set_title(f"{TIME_DECISION_LIST[TIME_DECISION_INDEX]} # of {sev} Crashes")
        ax.set_ylabel("# of Crashes")


    #horizontal line for 0%
    ax.axhline(0, color='gray', linestyle='--', linewidth=1)
    ax.legend()

    # #symlog scale for y-axis (handles negatives)
    # ax.set_yscale('symlog', linthresh=50)  # linthresh=50 makes non-drastic changes appear more linear (<50% absolute value)

    # set limits on Y
    # ax.set_ylim(-50, 50)

# Common x-axis label
plt.xlabel("Year")
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()

