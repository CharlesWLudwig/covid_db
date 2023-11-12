import pymysql
import os
import csv
from dotenv import load_dotenv
import sqlalchemy
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

table_1 = os.getenv('TABLE1')
table_2 = os.getenv('TABLE2')
table_3 = os.getenv('TABLE3')

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
brazil_group.to_csv('datasets/groups/brazil_group.csv')
china_group.to_csv('datasets/groups/china_group.csv')
egypt_group.to_csv('datasets/groups/egypt_group.csv')
sweden_group.to_csv('datasets/groups/sweden_group.csv')
usa_group.to_csv('datasets/groups/usa_group.csv')
       
# Extracting countries by their populations and number of cities
merged_dataframe.groupby("CountryName")["CountryPopulation"].value_counts().to_csv('datasets/country_by_population_and_cities.csv')

country_population = merged_dataframe.groupby("CountryName")["CountryPopulation"].value_counts()

country_population_dict = country_population.to_dict()

print(type(country_population))

brazil_dict = {}
china_dict = {}
egypt_dict = {}
sweden_dict = {}
usa_dict = {}

for key, value in country_population_dict.items():
    # Separating keys in tuple variable 'key'
    key_1, key_2 = key

    # For Countries bordering Brazil
    if key_1 == "Argentina":
        brazil_dict[key_1] = value      

    if key_1 == "Bolivia":
        brazil_dict[key_1] = value

    if key_1 == "Colombia":
        brazil_dict[key_1] = value

    if key_1 == "French Guiana":
        brazil_dict[key_1] = value

    if key_1 == "Guyana":
        brazil_dict[key_1] = value

    if key_1 == "Paraguay":
        brazil_dict[key_1] = value

    if key_1 == "Peru":
        brazil_dict[key_1] = value

    if key_1 == "Suriname":
        brazil_dict[key_1] = value

    if key_1 == "Uruguay":
        brazil_dict[key_1] = value

    if key_1 == "Venezuela":
        brazil_dict[key_1] = value

    # For Countries bordering China
    if key_1 == "Afghanistan":
        china_dict[key_1] = value

    if key_1 == "Bhutan":
        china_dict[key_1] = value
        
    if key_1 == "India":
        china_dict[key_1] = value
        
    if key_1 == "Kazakstan":
        china_dict[key_1] = value
        
    if key_1 == "North Korea":
        china_dict[key_1] = value
        
    if key_1 == "Kyrgyzstan":
        china_dict[key_1] = value
        
    if key_1 == "Laos":
        china_dict[key_1] = value
        
    if key_1 == "Mongolia":
        china_dict[key_1] = value
        
    if key_1 == "Myanmar":
        china_dict[key_1] = value
        
    if key_1 == "Burma":
        china_dict[key_1] = value
        
    if key_1 == "Pakistan":
        china_dict[key_1] = value
        
    if key_1 == "Russia":
        china_dict[key_1] = value
        
    if key_1 == "Mongolia":
        china_dict[key_1] = value
        
    if key_1 == "Tajikistan":
        china_dict[key_1] = value
        
    if key_1 == "Vietnam":
        china_dict[key_1] = value
        
    # For Countries bordering Egypt
    if key_1 == "Israel":
        egypt_dict[key_1] = value

    if key_1 == "Libya":
        egypt_dict[key_1] = value

    if key_1 == "Sudan":
        egypt_dict[key_1] = value

    # For Countries bordering Sweden
    if key_1 == "Finland":
        sweden_dict[key_1] = value

    if key_1 == "Norway":
        sweden_dict[key_1] = value

    # For Countries bordering USA
    if key_1 == "Canada":
        usa_dict[key_1] = value

    if key_1 == "Mexico":
        usa_dict[key_1] = value

print(brazil_dict)
print(china_dict)
print(egypt_dict)
print(sweden_dict)
print(usa_dict)

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

merged_dataframe.to_csv('datasets/all_countries.csv')

"""
# Original Tables from Database
result_dataFrame1.to_csv('datasets/DF_1.csv')
result_dataFrame2.to_csv('datasets/DF_2.csv')
result_dataFrame3.to_csv('datasets/DF_3.csv')
"""