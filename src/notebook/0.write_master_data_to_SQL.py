# Databricks notebook source
'''
Purpose: Read all the master data .csv files in the RefData folder and write them into SQL following SCD type I approach 
'''

# Import libraries and the source code
import os
dbutils.library.restartPython()
# Run all tests in the repository root.
notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
repo_root = os.path.dirname(os.path.dirname(notebook_path))
os.chdir(f'/Workspace/{repo_root}')
from source.reference_data.file_type import *
from source.configuration.initial_configuration import *
from source.shared import *
from source.reference_data import *

config_azure_auth()

''' Define global variables
---------------------------'''
# Get the list of master files
master_files = dbutils.fs.ls(BLOB_CONTAINER_FOLDER_REFDATA_ABFSS)

# Define the distionary of master data files
master_data = {
    'CHP': {'SQL_table': 'CHP', 'primary_keys': ['CODE'], 'datetime_col': 1},
    'LongMaster': {'SQL_table': 'DimLongMaster', 'primary_keys': ['Attribute'], 'datetime_col': 0},
    'NRSCHCoefficients': {'SQL_table': 'dimNRSCHCoefficients', 'primary_keys': ['Metric'], 'datetime_col': 0},
    'NRSCHCols': {'SQL_table': 'dimNRSCHCols', 'primary_keys': ['Tier'], 'datetime_col': 0},
    'NRSCHColTableNames': {'SQL_table': 'dimNRSCHColTableNames', 'primary_keys': ['Metric'], 'datetime_col': 0},
    'Tier': {'SQL_table': 'DIM_Tier', 'primary_keys': ['ID'], 'datetime_col': 0},
    'chpcontextual': {'SQL_table': 'chpcontextual', 'primary_keys': ['ChpId', 'Year'], 'datetime_col': 1}
}
''' END Define global variables
---------------------------'''

''' Process each master file type
---------------------------'''
if len(master_files) != 0:
    # Process each file in the RefData folder
    for master_file in master_files:
        # Get the file name before '-' to detect the master file: CHP or Tier, etc.
        file_type = master_file.name.split('-')[0]
        if file_type in master_data:
            print("Loading master files into SQL:", file_type)
            df_csv_file = spark.read.option("header", "true").csv(master_file.path) # The master .csv file
            if file_type == 'CHP':
                df_csv_file = df_csv_file.withColumn('CODE',regexp_replace(col('CODE'),' ','0'))
            # df_sql_table = read_table_from_database(SQL_SERVER_URL,master_data[file_type]['SQL_table']) # The data from SQL table
            overwrite_table(df_csv_file, master_data[file_type]['primary_keys'], SQL_CHIA_UAT_DATABASE, master_data[file_type]['SQL_table'], master_data[file_type]['datetime_col'])
            dbutils.fs.mv(master_file.path, BLOB_CONTAINER_PROCESS_FOLDER_REFDATA_ABFSS)
''' END Process each master file type
---------------------------'''