import duckdb #DuckDB will process the data with SQL
from pathlib import Path # we use Path to establish a location for the folder where our collection of monthly data files lives
from matplotlib import pyplot as plt #plotting library to graph info visually
from enum import Enum #make enumerated keywords (words with numerical values)
import pandas as pd #pandas to work with smaller result summary tables

#dictionary to match raw column names with legible names column_dict[name] -> column number
column_dict = {
    "ride_id": "column00",
    "rideable_type": "column01",
    "start time" : "column02",
    "end time" : "column03",
    "start_name" : "column04",
    "start_ID" : "column05",
    "end_name" : "column06",
    "end_ID" : "column07",
    "start_lat" : "column08",
    "start_long" : "column09",
    "end_lat" : "column10",
    "end_long" : "column11",
    "rider_type" : "column12",
    "start_nta" : "column13",
    "end_nta" : "column14"
}   

#escape apostrophes to not have them break in SQL query 
#(many neighborhoods have apostrophes, like randall's island)
def escape_apostrophes(lst):
    for i in range(len(lst)):
        lst[i] = lst[i].replace("'", "''")


#list of ntas (long)
crz_nta_list = [
    "Financial District-Battery Park City"
    ,"Tribeca-Civic Center"
    ,"The Battery-Governors Island-Ellis Island-Liberty Island"
    ,"SoHo-Little Italy-Hudson Square"
    ,"Greenwich Village"
    ,"West Village"
    ,"Chinatown-Two Bridges"
    ,"Lower East Side"
    ,"East Village"
    ,"Chelsea-Hudson Yards"
    ,"Hell's Kitchen"
    ,"Midtown South-Flatiron-Union Square"
    ,"Midtown-Times Square"
    ,"Stuyvesant Town-Peter Cooper Village"
    ,"Gramercy"
    ,"Murray Hill-Kips Bay"
    ,"East Midtown-Turtle Bay"
    ,"United Nations"
]

other_mnh_nta_list = [
    "Upper West Side-Lincoln Square"
    ,"Upper West Side (Central)"
    ,"Upper West Side-Manhattan Valley"
    ,"Upper East Side-Lenox Hill-Roosevelt Island"
    ,"Upper East Side-Carnegie Hill"
    ,"Upper East Side-Yorkville"
    ,"Morningside Heights"
    ,"Manhattanville-West Harlem"
    ,"Hamilton Heights-Sugar Hill"
    ,"Harlem (South)"
    ,"Harlem (North)"
    ,"East Harlem (South)"
    ,"East Harlem (North)"
    ,"Randall's Island"
    ,"Washington Heights (South)"
    ,"Washington Heights (North)"
    ,"Inwood"
    ,"Highbridge Park"
    ,"Inwood Hill Park"
    ,"Central Park"
]

bk_nta_list = [
    "Greenpoint"
    ,"Williamsburg"
    ,"South Williamsburg"
    ,"East Williamsburg"
    ,"Brooklyn Heights"
    ,"Downtown Brooklyn-DUMBO-Boerum Hill"
    ,"Fort Greene"
    ,"Clinton Hill"
    ,"Brooklyn Navy Yard"
    ,"Bedford-Stuyvesant (West)"
    ,"Bedford-Stuyvesant (East)"
    ,"Bushwick (West)"
    ,"Bushwick (East)"
    ,"The Evergreens Cemetery"
    ,"Cypress Hills"
    ,"East New York (North)"
    ,"East New York-New Lots"
    ,"Spring Creek-Starrett City"
    ,"East New York-City Line"
    ,"Highland Park-Cypress Hills Cemeteries (South)"
    ,"Carroll Gardens-Cobble Hill-Gowanus-Red Hook"
    ,"Park Slope"
    ,"Windsor Terrace-South Slope"
    ,"Sunset Park (West)"
    ,"Sunset Park (Central)"
    ,"Green-Wood Cemetery"
    ,"Prospect Heights"
    ,"Crown Heights (North)"
    ,"Lincoln Terrace Park"
    ,"Crown Heights (South)"
    ,"Prospect Lefferts Gardens-Wingate"
    ,"Bay Ridge"
    ,"Dyker Heights"
    ,"Fort Hamilton"
    ,"Dyker Beach Park"
    ,"Bensonhurst"
    ,"Bath Beach"
    ,"Gravesend (West)"
    ,"Sunset Park (East)-Borough Park (West)"
    ,"Borough Park"
    ,"Kensington"
    ,"Mapleton-Midwood (West)"
    ,"Gravesend (South)"
    ,"Coney Island-Sea Gate"
    ,"Brighton Beach"
    ,"Calvert Vaux Park"
    ,"Flatbush"
    ,"Flatbush (West)-Ditmas Park-Parkville"
    ,"Midwood"
    ,"Gravesend (East)-Homecrest"
    ,"Madison"
    ,"Sheepshead Bay-Manhattan Beach-Gerritsen Beach"
    ,"Ocean Hill"
    ,"Brownsville"
    ,"East Flatbush-Erasmus"
    ,"East Flatbush-Farragut"
    ,"East Flatbush-Rugby"
    ,"East Flatbush-Remsen Village"
    ,"Holy Cross Cemetery"
    ,"Flatlands"
    ,"Marine Park-Mill Basin-Bergen Beach"
    ,"Canarsie"
    ,"Marine Park-Plumb Island"
    ,"McGuire Fields"
    ,"Canarsie Park & Pier"
    ,"Prospect Park"
    ,"Barren Island-Floyd Bennett Field"
    ,"Jamaica Bay (West)"
    ,"Shirley Chisholm State Park"
]

bronx_nta_list = [
    "Mott Haven-Port Morris"
    ,"Melrose"
    ,"Hunts Point"
    ,"Longwood"
    ,"North & South Brother Islands"
    ,"Morrisania"
    ,"Claremont Village-Claremont (East)"
    ,"Crotona Park East"
    ,"Crotona Park"
    ,"Concourse-Concourse Village"
    ,"Highbridge"
    ,"Mount Eden-Claremont (West)"
    ,"Yankee Stadium-Macombs Dam Park"
    ,"Claremont Park"
    ,"University Heights (South)-Morris Heights"
    ,"Mount Hope"
    ,"Fordham Heights"
    ,"West Farms"
    ,"Tremont"
    ,"Belmont"
    ,"University Heights (North)-Fordham"
    ,"Bedford Park"
    ,"Norwood"
    ,"Kingsbridge Heights-Van Cortlandt Village"
    ,"Kingsbridge-Marble Hill"
    ,"Riverdale-Spuyten Duyvil"
    ,"Soundview-Bruckner-Bronx River"
    ,"Soundview-Clason Point"
    ,"Castle Hill-Unionport"
    ,"Parkchester"
    ,"Soundview Park"
    ,"Westchester Square"
    ,"Throgs Neck-Schuylerville"
    ,"Pelham Bay-Country Club-City Island"
    ,"Co-op City"
    ,"Hart Island"
    ,"Ferry Point Park-St. Raymond Cemetery"
    ,"Pelham Parkway-Van Nest"
    ,"Morris Park"
    ,"Pelham Gardens"
    ,"Allerton"
    ,"Hutchinson Metro Center"
    ,"Williamsbridge-Olinville"
    ,"Eastchester-Edenwald-Baychester"
    ,"Wakefield-Woodlawn"
    ,"Woodlawn Cemetery"
    ,"Van Cortlandt Park"
    ,"Bronx Park"
    ,"Pelham Bay Park"
]

queens_nta_list = [
    "Astoria (North)-Ditmars-Steinway"
    ,"Old Astoria-Hallets Point"
    ,"Astoria (Central)"
    ,"Astoria (East)-Woodside (North)"
    ,"Queensbridge-Ravenswood-Dutch Kills"
    ,"Rikers Island"
    ,"Sunnyside Yards (North)"
    ,"St. Michael's Cemetery"
    ,"Astoria Park"
    ,"Long Island City-Hunters Point"
    ,"Sunnyside"
    ,"Woodside"
    ,"Sunnyside Yards (South)"
    ,"Calvary & Mount Zion Cemeteries"
    ,"Jackson Heights"
    ,"East Elmhurst"
    ,"North Corona"
    ,"Elmhurst"
    ,"Corona"
    ,"Maspeth"
    ,"Ridgewood"
    ,"Glendale"
    ,"Middle Village"
    ,"Mount Olivet & All Faiths Cemeteries"
    ,"Middle Village Cemetery"
    ,"St. John Cemetery"
    ,"Highland Park-Cypress Hills Cemeteries (North)"
    ,"Rego Park"
    ,"Forest Hills"
    ,"College Point"
    ,"Whitestone-Beechhurst"
    ,"Bay Terrace-Clearview"
    ,"Murray Hill-Broadway Flushing"
    ,"East Flushing"
    ,"Queensboro Hill"
    ,"Flushing-Willets Point"
    ,"Fort Totten"
    ,"Kissena Park"
    ,"Kew Gardens Hills"
    ,"Pomonok-Electchester-Hillcrest"
    ,"Fresh Meadows-Utopia"
    ,"Jamaica Estates-Holliswood"
    ,"Jamaica Hills-Briarwood"
    ,"Mount Hebron & Cedar Grove Cemeteries"
    ,"Cunningham Park"
    ,"Kew Gardens"
    ,"Richmond Hill"
    ,"South Richmond Hill"
    ,"Ozone Park (North)"
    ,"Woodhaven"
    ,"South Ozone Park"
    ,"Ozone Park"
    ,"Howard Beach-Lindenwood"
    ,"Spring Creek Park"
    ,"Auburndale"
    ,"Bayside"
    ,"Douglaston-Little Neck"
    ,"Oakland Gardens-Hollis Hills"
    ,"Alley Pond Park"
    ,"Jamaica"
    ,"South Jamaica"
    ,"Baisley Park"
    ,"Springfield Gardens (North)-Rochdale Village"
    ,"St. Albans"
    ,"Hollis"
    ,"Glen Oaks-Floral Park-New Hyde Park"
    ,"Bellerose"
    ,"Queens Village"
    ,"Cambria Heights"
    ,"Laurelton"
    ,"Springfield Gardens (South)-Brookville"
    ,"Rosedale"
    ,"Montefiore Cemetery"
    ,"Far Rockaway-Bayswater"
    ,"Rockaway Beach-Arverne-Edgemere"
    ,"Breezy Point-Belle Harbor-Rockaway Park-Broad Channel"
    ,"Rockaway Community Park"
    ,"LaGuardia Airport"
    ,"Flushing Meadows-Corona Park"
    ,"Forest Park"
    ,"John F. Kennedy International Airport"
    ,"Jamaica Bay (East)"
    ,"Jacob Riis Park-Fort Tilden-Breezy Point Tip"
]

si_nta_list = [
    "St. George-New Brighton"
    ,"Tompkinsville-Stapleton-Clifton-Fox Hills"
    ,"Rosebank-Shore Acres-Park Hill"
    ,"West New Brighton-Silver Lake-Grymes Hill"
    ,"Westerleigh-Castleton Corners"
    ,"Port Richmond"
    ,"Mariner's Harbor-Arlington-Graniteville"
    ,"Snug Harbor"
    ,"Grasmere-Arrochar-South Beach-Dongan Hills"
    ,"New Dorp-Midland Beach"
    ,"Todt Hill-Emerson Hill-Lighthouse Hill-Manor Heights"
    ,"New Springville-Willowbrook-Bulls Head-Travis"
    ,"Freshkills Park (North)"
    ,"Oakwood-Richmondtown"
    ,"Great Kills-Eltingville"
    ,"Arden Heights-Rossville"
    ,"Annadale-Huguenot-Prince's Bay-Woodrow"
    ,"Tottenville-Charleston"
    ,"Freshkills Park (South)"
    ,"Fort Wadsworth"
    ,"Hoffman & Swinburne Islands"
    ,"Miller Field"
    ,"Great Kills Park"
]

#apply escape apostrophe function for all borough lists
escape_apostrophes(crz_nta_list)
escape_apostrophes(other_mnh_nta_list)
escape_apostrophes(bk_nta_list)
escape_apostrophes(bronx_nta_list)
escape_apostrophes(queens_nta_list)
escape_apostrophes(si_nta_list)

#make non-mnh and non-cbd lists
non_mnh_nta_list = si_nta_list + bk_nta_list + bronx_nta_list + queens_nta_list
non_cbd_nta_list = non_mnh_nta_list + other_mnh_nta_list

#TEMPLATE TO FILL IN VARIABLE NAMES BY YEAR
list_of_years = ["2024", "2025"] #EDIT HERE TO UPDATE THE LIST OF YEARS TO CHECK
list_of_source_files_csv = {} #source file dictionary, keeps track of source files through the years
list_of_source_files_parquet = {} #source file dictionary, keeps track of source files through the years
list_of_tables = {} #table dictionary, keeps track of tables through the years

for year in list_of_years: #iterate through the years to create the variable names (excl. parquet)

    list_of_source_files_csv[year] = Path(f"{year}-citibike-tripdata-full.csv") #ex: 2024-citibike-tripdata-full.csv 

    list_of_tables[year] = f"citibike_{year}"



#CREATE DATABASE, WILL BE CLOSED AT THE END OF RUN
duck_citibike_connect = duckdb.connect(database="citibike.duckdb") 

#INSTALL/LOAD SPATIAL CONNECTION
duck_citibike_connect.execute("INSTALL spatial;")
duck_citibike_connect.execute("LOAD spatial;")


#CHECK IF PARQUET HAS ALREADY BEEN CREATED, IF NOT:
file_path = Path("2024-citibike-tripdata-full.parquet")
if (not (file_path.exists())):

    for year in list_of_years: #iterate through the years to convert csv to parquet
        csv_path = list_of_source_files_csv[year]

        duck_citibike_connect.execute(f"""
                COPY (
                    SELECT *
                    FROM read_csv_auto(
                    '{csv_path}',
                    types={{'column05': 'VARCHAR'}}
                )
            ) TO '{year}-citibike-tripdata-full.parquet' (FORMAT PARQUET);
        """)


for year in list_of_years: #iterate through the years to create the parquet file veriable names

    list_of_source_files_parquet[year] = Path(f"{year}-citibike-tripdata-full.parquet") #ex: 2024-citibike-tripdata-full.parquet



for year in list_of_years: #iterate through the years to load the parquet into duckdb SQL for each year
    table_name = list_of_tables[year]
    parquet_path = list_of_source_files_parquet[year]


    # IF ALREADY EXISTS, BUT DATA IS OLD, USER RESPONSIBILITY TO REPLACE
    duck_citibike_connect.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} AS 
        SELECT *
        FROM read_parquet('{parquet_path}')
    """)

#ACTUAL ANALYSIS BEGINS HERE

#NTA table in DuckDB database
duck_citibike_connect.execute("""
    CREATE OR REPLACE TABLE ntas AS
    SELECT *
    FROM ST_Read('NTAs/geo_export_bd71e9cd-14bd-435b-99c8-b573a94a1dc8.shp');
""")

neighborhood_list = duck_citibike_connect.execute("DESCRIBE ntas").fetchall()

for year in list_of_years: #iterate through the years to read/process duckdb tables
    table_name = list_of_tables[year]
    
    #add start and end ntas to yearly tables, set them to a spatial join from nta shapefile
    duck_citibike_connect.execute(f"""
        ALTER TABLE {table_name}
        ADD COLUMN IF NOT EXISTS {column_dict['start_nta']} VARCHAR;
    """)

    duck_citibike_connect.execute(f"""
        ALTER TABLE {table_name}
        ADD COLUMN IF NOT EXISTS {column_dict['end_nta']} VARCHAR;
    """)

    #UPDATE THE START NTA COLUMN TO HAVE AN ACTUAL NEIGHBORHOOD IN IT! 
    #THIS IS A VERY TAXING PROCESS (10M+, USE ONLY WHEN BRAND NEW DATA SHOWS UP)
    test_start_nta = duck_citibike_connect.execute(f"""
        SELECT {column_dict['start_nta']}
        FROM {table_name}
        ORDER BY rowid
        LIMIT 1
    """).fetchone()[0]

    if test_start_nta is None:
        duck_citibike_connect.execute(f"""
            UPDATE {table_name}
            SET {column_dict['start_nta']} = ntas.ntaname
            FROM ntas
            WHERE ST_Within(
                ST_Point({table_name}.{column_dict['start_long']}, {table_name}.{column_dict['start_lat']}),
                ntas.geom
            );
        """)

    #UPDATE THE END NTA COLUMN TO HAVE AN ACTUAL NEIGHBORHOOD IN IT! 
    #THIS IS A VERY TAXING PROCESS (10M+, USE ONLY WHEN BRAND NEW DATA SHOWS UP)
    test_end_nta = duck_citibike_connect.execute(f"""
        SELECT {column_dict['end_nta']}
        FROM {table_name}
        ORDER BY rowid
        LIMIT 1
    """).fetchone()[0]

    if test_end_nta is None:
        duck_citibike_connect.execute(f"""
            UPDATE {table_name}
            SET {column_dict['end_nta']} = ntas.ntaname
            FROM ntas
            WHERE ST_Within(
                ST_Point({table_name}.{column_dict['end_long']}, {table_name}.{column_dict['end_lat']}),
                ntas.geom
            );
        """)

    #object to store summary data
    summary_info = []

    for month in range(1, 13):
        non_cbd_to_cbd = duck_citibike_connect.execute(f"""
            SELECT COUNT(*) FROM {table_name} 
            WHERE {column_dict['start_nta']} = ANY(?)
            AND {column_dict['end_nta']} = ANY(?)
            AND EXTRACT(MONTH FROM {column_dict['start time']}) = {month}
        """, [non_cbd_nta_list, crz_nta_list]).fetchone()[0] #take counts of rides that started outside of cbd, ended in cbd

        cbd_to_non_cbd = duck_citibike_connect.execute(f"""
            SELECT COUNT(*) FROM {table_name} 
            WHERE {column_dict['start_nta']} = ANY(?)
            AND {column_dict['end_nta']} = ANY(?)
            AND EXTRACT(MONTH FROM {column_dict['start time']}) = {month}
        """, [crz_nta_list, non_cbd_nta_list]).fetchone()[0] #take counts of rides that started in cbd, ended outside of cbd

        summary_info.append({
            'year': year,
            'month': month,
            'non_cbd_to_cbd': non_cbd_to_cbd,
            'cbd_to_non_cbd': cbd_to_non_cbd
        })


df = pd.DataFrame(summary_info)

print(df)
#CLOSE DATABASE
duck_citibike_connect.close()