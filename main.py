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

result_dataFrame1.rename(
    columns={
        'Name': 'CityName',
        'District': 'CityDistrict',
        'Population': 'CityPopulation'
    }, 
    inplace=True
)

# print(result_dataFrame1.columns)

result_dataFrame2.rename(
    columns={
        'Name': 'CountryName',
        'Code': 'CountryCode',
        'Population': 'CountryPopulation'
    }, 
    inplace=True
)

# print(result_dataFrame2.columns)

# print(result_dataFrame3.columns)

merged_dataframe = result_dataFrame1.merge(
    result_dataFrame2,on='CountryCode'
    ).merge(
        result_dataFrame3,on='CountryCode'
    )

# print(merged_dataframe)

by_country = merged_dataframe.groupby("CountryName")

"""
brazil_group = by_country.groups["Brazil"]
china_group = by_country.groups["China"]
egypt_group = by_country.groups["Egypt"]
sweden_group = by_country.groups["Sweden"]
usa_group = by_country.groups["United States"]
"""

brazil_group = by_country.get_group("Brazil")
china_group = by_country.get_group("China")
egypt_group = by_country.get_group("Egypt")
sweden_group = by_country.get_group("Sweden")
usa_group = by_country.get_group("United States")
"""
print(brazil_group)
print(china_group)
print(egypt_group)
print(sweden_group)
print(usa_group)
"""
print(merged_dataframe.columns)
       
merged_dataframe.groupby("CountryName")["CountryPopulation"].value_counts().to_csv('datasets/merged_grouped.csv')

"""
print(result_dataFrame1.head())
print(result_dataFrame2.head())
print(result_dataFrame3.head())

result_dataFrame1.to_csv('datasets/DF_1.csv')
result_dataFrame2.to_csv('datasets/DF_2.csv')
result_dataFrame3.to_csv('datasets/DF_3.csv')
"""
connection.close()