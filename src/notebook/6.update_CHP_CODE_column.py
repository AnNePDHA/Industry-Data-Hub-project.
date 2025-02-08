# Databricks notebook source

import os
os.chdir('/Workspace/src')
from source.shared import *
import pyodbc

config_azure_auth()

# Connect to SQL database
# conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER=' + SQL_SERVER_NAME + ';DATABASE='+ 'chia-prod' +';UID=' + SQL_USERNAME + ';PWD='+ SQL_PASSWORD)
# cursor = conn.cursor()
tokenstruct = get_access_token_for_conn()
SQL_COPT_SS_ACCESS_TOKEN = 1256
conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server}; SERVER=; DATABASE=', attrs_before = {SQL_COPT_SS_ACCESS_TOKEN: tokenstruct})    
cursor = conn.cursor()

sql_query = """
UPDATE chris 
SET [CHP CODE] = CHP.CODE 
FROM CHP 
WHERE chris.[What is the name of your organisation?] = CHP.[CHP Name]

UPDATE chapr
SET [CHP CODE] = CHP.CODE
FROM CHP 
WHERE chapr.[CHP] = CHP.[CHP Name]
"""
try:
    cursor.execute(sql_query)
    cursor.commit()
    cursor.close()
    conn.close()
    dbutils.notebook.exit("we're good here")
except Exception as e:
    print(e)
    cursor.close()
    conn.close()
    dbutils.notebook.exit(e)