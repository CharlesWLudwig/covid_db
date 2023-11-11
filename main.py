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

print(result_dataFrame1.head())
print(result_dataFrame2.head())
print(result_dataFrame3.head())

connection.close()

result_dataFrame1.to_csv('datasets/DF_1.csv')
result_dataFrame2.to_csv('datasets/DF_2.csv')
result_dataFrame3.to_csv('datasets/DF_3.csv')