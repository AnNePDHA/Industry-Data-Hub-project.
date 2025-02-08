# Databricks notebook source
'''
Purpose: translate rule .csv files into SQL. This section will:
  -  read all the files in the input/MASTERDATA/DATAFIELD folder.
  -  Read all data files into the list all_files
  -  Write and update the rules in the SQL file_type table
  -  Move the rule files into process folder: processed/MASTERDATA/DATAFIELD 
'''

import os
os.chdir(f'/Workspace/src')
from source.reference_data.file_type import *
from source.configuration.initial_configuration import *
from source.shared import *
from source.reference_data import *


config_azure_auth()
folders = dbutils.fs.ls(BLOB_CONTAINER_FOLDER_INPUT_ABFSS)
rule_files = []

for folder in folders:
  #just select the files in CHIA folder and CHP folder
    if folder.name == 'MASTERDATA/':
        rule_files += read_info_files( folder.path + 'DATAFIELD/', 'DATAFIELD/' )
         
length = len(rule_files)
# print(length)

if len(rule_files) > 0:
    # Get current time
    current_time = datetime.now()
    
    # Write query and initial value list for updating records in SQL file_type table: update for column current_id and Effective_to
    query_update = """
    UPDATE file_type
    SET current_id = 0, Effective_to = (?)
    WHERE FileType = (?) AND current_id = 1
    """
    value_update = []
    
    # Write query and initial value list for writing new rules/new rule versions into the SQL file_type table
    query_write = """
    INSERT INTO file_type 
    (FileType, ColumnName, Description, CreatedBy, CreatedDate, UpdatedBy, UpdatedDate, Effective_from, Active, current_id, DataType, CanBeNull, PrivateIdentifier, BusinessKey, ReferenceData, UseInValidation, [Alternate Name])
    VALUES (?, ?, ?, ?, CAST( ? AS datetime), ?, CAST( ? AS datetime), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    value_write = []
    
    # Sort the rule file list by the creation date
    rule_files.sort(key = creation_date)
    
    # Connect to SQL database   
    
    tokenstruct = get_access_token_for_conn()
    SQL_COPT_SS_ACCESS_TOKEN = 1256
    conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server}; SERVER=; DATABASE=', attrs_before = {SQL_COPT_SS_ACCESS_TOKEN: tokenstruct})    
    cursor = conn.cursor()

    # print(rule_files)
    for file in rule_files:
        # Read the rule file
        df = spark.read.option("header", "true").option("inferSchema", "true").csv(file['file_path'])
        file_name = file['file_path'].split('/')[-1]

        if df.rdd.isEmpty():
            # print(file['file_path'],'has not content')
            dbutils.fs.mv(file['file_path'], f'abfss://{BLOB_CONTAINER}@{BLOB_STORAGE_URL}/error/MASTERDATA/DATAFIELD')
            continue

        if df.tail(1)[0]['File Type'] == 'chapr':
            # Lower and remove space all value in "Name" column
            df = df.withColumn("Name", regexp_replace(lower(col('Name'))," ",""))

            
        # Handle header value of file type in Name column
        df = df.withColumn("Name", regexp_replace(col('Name'), '"', ""))

        df_list = df.collect()  # Read list row: [Row(ID='...',....),...]
        
        # Process each row of the rule file
        for row in range( df.count() ):
            # Write value list for writing new rules/new rule versions into the SQL file_type table
            value_write.append( (df_list[row]['File Type'].replace(' ', ''), df_list[row]['Name'], df_list[row]['Description'], df_list[row]['Created By'], df_list[row]['Created Date'], df_list[row]['Updated By'], df_list[row]['Updated Date'], current_time, df_list[row]['Active'], 1, df_list[row]['Data Type'], df_list[row]['Can Be Null'], df_list[row]['Private Identifier'], df_list[row]['Business Key'], df_list[row]['Reference Data'], df_list[row]['UseInValidation'], df_list[row][' Alternate Name']) )

        # Write value list for updating records in SQL file_type table: update for column current_id and Effective_to
        value_update.append( (current_time, df_list[row]['File Type'].replace(' ', '')) )
     
        # Execute SQL commands
        try:
            cursor.executemany(query_update, value_update)
            cursor.executemany(query_write, value_write)
            conn.commit()

            # Overwrite rule file with df 
            df.toPandas().to_csv(f'/dbfs/mnt/datalake/input/MASTERDATA/DATAFIELD/{file_name}', index = False)

            print(file['file_path'],'INSERT successfully!')
           
        except Exception as e:
            # Move files in input/MASTERDATA/DATAFIELD into error folder
            dbutils.fs.mv(file['file_path'], f'abfss://{BLOB_CONTAINER}@{BLOB_STORAGE_URL}/error/MASTERDATA/DATAFIELD')

            cursor.close()
            conn.close()
            # Raise error and stop processing
            dbutils.notebook.exit('Error in ',file['file_path'],f' : {e}')
            
        # Return the 2 list to empty list to prepare for the next file
        value_update = []
        value_write = []
            
    # Closing SQL connections
    cursor.close()
    conn.close()