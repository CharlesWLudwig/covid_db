import pymysql
import os
import csv
from dotenv import load_dotenv
import sqlalchemy

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
connection.close()
