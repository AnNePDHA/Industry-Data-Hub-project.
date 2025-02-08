# Databricks notebook source
'''
Purpose: Process all the file_type files in the folder MASTERDATA/DATAFIELD. 
o	If the file is in the exception_list, do no more.
o	If the file in file_path does not have any business key: add an error into SQL table error_logging and move the file to process folder, do no more.
o	Call get_primary_keys function to get the list of business keys from the file type file.
o	Check if there is a table in SQL corresponding for this file or not.
o	If no, call create_table_from_file_type
o	If yes, call sql_update_table
'''

import os
dbutils.library.restartPython()
os.chdir('/Workspace/src')
from source.shared import *
from source.configuration import *
from source.reference_data import *


config_azure_auth()
folders = dbutils.fs.ls(BLOB_CONTAINER_FOLDER_INPUT_ABFSS)
rule_files = []
# print(folders)

for folder in folders:
  #just select the files in CHIA folder and CHP folder
    if folder.name == 'MASTERDATA/':
        rule_files += read_info_files( folder.path + 'DATAFIELD/', 'DATAFIELD/' )

print(rule_files)         
length = len(rule_files)

if length > 0:
    for f in rule_files:
        print(f['file_path'])
        flag = sql_file_type_check(f['file_path'])
        if flag:
            # Move files in input/MASTERDATA/DATAFIELD into processed folder
            dbutils.fs.mv(f['file_path'], f'abfss://{BLOB_CONTAINER}@{BLOB_STORAGE_URL}/processed/MASTERDATA/DATAFIELD')