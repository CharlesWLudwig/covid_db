import pymysql
import os
from pprint import pprint
import csv
from dotenv import load_dotenv
import pandas as pd

load_dotenv()

connection = pymysql.connect(
    host=os.getenv("LOCAL_HOST"),
    database=os.getenv("LOCAL_DATABASE"),
    user= os.getenv("LOCAL_USERNAME"),
    password=os.getenv("LOCAL_PASSWORD"),
)

cursor = connection.cursor()
cursor.execute("select @@version ")

version = cursor.fetchone()
if version:
    print('Running version: ', version)
else:
    print('Not connected.')

# To protect against SQL Injection attacks, parameterizing my queries
table_1 = os.getenv('TABLE1') # City
table_2 = os.getenv('TABLE2') # Country
table_3 = os.getenv('TABLE3') # CountryLanguage

try:
    query = f"Select * from {table_1};"
    result_dataFrame1 = pd.read_sql(query,connection)
except Exception as e:
    print(str(e))

try:
    query = f"Select * from {table_2};"
    result_dataFrame2 = pd.read_sql(query,connection)
except Exception as e:
    print(str(e))

try:
    query = f"Select * from {table_3};"
    result_dataFrame3 = pd.read_sql(query,connection)
except Exception as e:
    print(str(e))

connection.close()

# Renaming columns from tables for consistency across dataframes

result_dataFrame1.rename(
    columns={
        'Name': 'CityName',
        'District': 'CityDistrict',
        'Population': 'CityPopulation'
    }, 
    inplace=True
)

result_dataFrame2.rename(
    columns={
        'Name': 'CountryName',
        'Code': 'CountryCode',
        'Population': 'CountryPopulation'
    }, 
    inplace=True
)

merged_dataframe = result_dataFrame1.merge(
    result_dataFrame2,on='CountryCode'
    ).merge(
        result_dataFrame3,on='CountryCode'
    )

# Creating groups of different countries for vaccine allocations
by_country = merged_dataframe.groupby("CountryName")

brazil_group = by_country.get_group("Brazil")
china_group = by_country.get_group("China")
egypt_group = by_country.get_group("Egypt")
sweden_group = by_country.get_group("Sweden")
usa_group = by_country.get_group("United States")

# Saving different groups to different CSV files (for posterity)
brazil_group.to_csv('brazil_group.csv')
china_group.to_csv('china_group.csv')
egypt_group.to_csv('egypt_group.csv')
sweden_group.to_csv('sweden_group.csv')
usa_group.to_csv('usa_group.csv')
       
# Extracting countries by their populations and number of cities
merged_dataframe.groupby("CountryName")["CountryPopulation"].value_counts().to_csv('country_by_population_and_cities.csv')

country_population = merged_dataframe.groupby("CountryName")["CountryPopulation"].value_counts()

country_population_dict = country_population.to_dict()

brazil_dict = {}
china_dict = {}
egypt_dict = {}
sweden_dict = {}
usa_dict = {}

# Extracting individual country's population weights in the respective vaccine allocation regions
for key, value in country_population_dict.items():
    # Separating keys in tuple variable 'key'
    key_1, key_2 = key

    # For Countries bordering Brazil
    if key_1 == "Argentina":
        brazil_dict[key_1] = key_2 

    if key_1 == "Bolivia":
        brazil_dict[key_1] = key_2 
        
    if key_1 == "Colombia":
        brazil_dict[key_1] = key_2 
        
    if key_1 == "French Guiana":
        brazil_dict[key_1] = key_2 
          
    if key_1 == "Guyana":
        brazil_dict[key_1] = key_2 

    if key_1 == "Paraguay":
        brazil_dict[key_1] = key_2 

    if key_1 == "Peru":
        brazil_dict[key_1] = key_2 

    if key_1 == "Suriname":
        brazil_dict[key_1] = key_2 

    if key_1 == "Uruguay":
        brazil_dict[key_1] = key_2

    if key_1 == "Venezuela":
        brazil_dict[key_1] = key_2  

    # For Countries bordering China
    if key_1 == "Afghanistan":
        china_dict[key_1] = key_2 

    if key_1 == "Bhutan":
        china_dict[key_1] = key_2 

    if key_1 == "India":
        china_dict[key_1] = key_2  
    if key_1 == "Kazakstan":
        china_dict[key_1] = key_2  
    if key_1 == "North Korea":
        china_dict[key_1] = key_2  
    if key_1 == "Kyrgyzstan":
        china_dict[key_1] = key_2 
    if key_1 == "Laos":
        china_dict[key_1] = key_2 
    if key_1 == "Mongolia":
        china_dict[key_1] = key_2 
    if key_1 == "Myanmar":
        china_dict[key_1] = key_2 

    if key_1 == "Burma":
        china_dict[key_1] = key_2

    if key_1 == "Pakistan":
        china_dict[key_1] = key_2 

    if key_1 == "Russia":
        china_dict[key_1] = key_2 

    if key_1 == "Mongolia":
        china_dict[key_1] = key_2 

    if key_1 == "Tajikistan":
        china_dict[key_1] = key_2 

    if key_1 == "Vietnam":
        china_dict[key_1] = key_2

    # For Countries bordering Egypt
    if key_1 == "Israel":
        egypt_dict[key_1] = key_2 

    if key_1 == "Libya":
        egypt_dict[key_1] = key_2 

    if key_1 == "Sudan":
        egypt_dict[key_1] = key_2 

    # For Countries bordering Sweden
    if key_1 == "Finland":
        sweden_dict[key_1] = key_2 

    if key_1 == "Norway":
        sweden_dict[key_1] = key_2  

    # For Countries bordering USA
    if key_1 == "Canada":
        usa_dict[key_1] = key_2

    if key_1 == "Mexico":
        usa_dict[key_1] = key_2  

pprint(brazil_dict)
pprint(china_dict)
pprint(egypt_dict)
pprint(sweden_dict)
pprint(usa_dict)

# This is a cluster of how many people per vaccine in the group of countries
# print(population_per_vaccine)

vaccine_allotment_dict = {
    "Brazil": 150000,
    "China": 200000,
    "Egypt": 50000, 
    "Sweden": 50000,
    "United States": 150000
}

total_vaccines_alloted = 0

for v_key, v_value in vaccine_allotment_dict.items():    
    if v_key == "Brazil":
        new_dict = {
            'Population': 0
        } 

        total_population_counter = 0
        counter_index = 0

        for key, value in brazil_dict.items():
#           print(f"{key} has {value} people")
            total_population_counter += value
            counter_index += 1
            new_dict['Population'] += value

        for key, value in new_dict.items():
            new_dict['Population'] = value

#        print(f"There are {total_population_counter} people in {counter_index} countries.")

        mean = total_population_counter / counter_index

        population_per_vaccine = total_population_counter / mean

        for key, value in brazil_dict.items():
            sum = v_value / population_per_vaccine
            print(f"{key}'s Vaccine allotment:  {sum}")
            print(f"\tPer city center, {key} will have {round(total_population_counter / value, 2)} people per vaccine")
            total_vaccines_alloted = sum * counter_index

        print(f"Total vaccines alloted for the {counter_index} countries in the {v_key} Group: {int(total_vaccines_alloted)}\n")

    if v_key == "China":
        new_dict = {
            'Population': 0
        } 

        total_population_counter = 0
        counter_index = 0

        for key, value in china_dict.items():
#           print(f"{key} has {value} people")
            total_population_counter += value
            counter_index += 1
            new_dict['Population'] += value

        for key, value in new_dict.items():
            new_dict['Population'] = value

#       print(f"There are {total_population_counter} people in {counter_index} countries.")

        mean = total_population_counter / counter_index

        population_per_vaccine = total_population_counter / mean

        for key, value in china_dict.items():
            sum = v_value / population_per_vaccine
            print(f"{key}'s Vaccine allotment:  {int(sum)}")
            print(f"\tPer city center, {key} will have {round(total_population_counter / value, 2)} people per vaccine")
            total_vaccines_alloted = sum * counter_index

        print(f"Total vaccines alloted for the {counter_index} countries in the {v_key} Group: {int(total_vaccines_alloted)}\n")

    if v_key == "Egypt":
        new_dict = {
            'Population': 0
        } 

        total_population_counter = 0
        counter_index = 0

        for key, value in egypt_dict.items():
#           print(f"{key} has {value} people")
            total_population_counter += value
            counter_index += 1
            new_dict['Population'] += value

        for key, value in new_dict.items():
            new_dict['Population'] = value

#       print(f"There are {total_population_counter} people in {counter_index} countries.")

        mean = total_population_counter / counter_index

        population_per_vaccine = total_population_counter / mean

        for key, value in egypt_dict.items():
            sum = v_value / population_per_vaccine
            print(f"{key}'s Vaccine allotment:  {sum}")
            print(f"\tPer city center, {key} will have {round(total_population_counter / value, 2)} people per vaccine")
            total_vaccines_alloted = sum * counter_index

        print(f"Total vaccines alloted for the {counter_index} countries in the {v_key} Group: {int(total_vaccines_alloted)}\n")

    if v_key == "Sweden":
        new_dict = {
            'Population': 0
        } 

        total_population_counter = 0
        counter_index = 0

        for key, value in sweden_dict.items():
#           print(f"{key} has {value} people")
            total_population_counter += value
            counter_index += 1
            new_dict['Population'] += value

        for key, value in new_dict.items():
            new_dict['Population'] = value

#       print(f"There are {total_population_counter} people in {counter_index} countries.")

        mean = total_population_counter / counter_index

        population_per_vaccine = total_population_counter / mean

        for key, value in sweden_dict.items():
            sum = v_value / population_per_vaccine
            print(f"{key}'s Vaccine allotment:  {sum}")
            print(f"\tPer city center, {key} will have {round(total_population_counter / value, 2)} people per vaccine")
            total_vaccines_alloted = sum * counter_index

        print(f"Total vaccines alloted for the {counter_index} countries in the {v_key} Group: {int(total_vaccines_alloted)}\n")

    if v_key == "United States":
        new_dict = {
            'Population': 0
        } 

        total_population_counter = 0
        counter_index = 0

        for key, value in usa_dict.items():
#           print(f"{key} has {value} people")
            total_population_counter += value
            counter_index += 1
            new_dict['Population'] += value

        for key, value in new_dict.items():
            new_dict['Population'] = value

#       print(f"There are {total_population_counter} people in {counter_index} countries.")

        mean = total_population_counter / counter_index

        population_per_vaccine = total_population_counter / mean

        for key, value in usa_dict.items():
            sum = v_value / population_per_vaccine
            print(f"{key}'s Vaccine allotment:  {sum}")
            print(f"\tPer city center, {key} will have {round(total_population_counter / value, 2)} people per vaccine")
            total_vaccines_alloted = sum * counter_index

        print(f"Total vaccines alloted for the {counter_index} countries in the {v_key} Group: {int(total_vaccines_alloted)}\n")

# Columns I deem to be irrelevant to data collection
merged_dataframe.drop(
    ['Capital',
     'Code2',
     'Language',
     'IsOfficial',
     'Percentage',
     'ID',
     'CityDistrict',
     'Continent',
     'Region',
     'IndepYear',
     'GNPOld',
     'LocalName'
    ], axis=1, inplace=True)

merged_dataframe.to_csv('all_countries.csv')

# Original Tables from Database
result_dataFrame1.to_csv('DF_1.csv')
result_dataFrame2.to_csv('DF_2.csv')
result_dataFrame3.to_csv('DF_3.csv')