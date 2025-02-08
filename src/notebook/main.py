# Databricks notebook source
'''
Purpose: The main function, running all the processes
'''


import os
os.chdir('/Workspace/src')

from source.configuration import *
from source.shared import *

JOB_TIMEOUT_SECONDS = 1000000

# 1.1 Load master data from
#  reference data
print('1.1. Load master data..')
dbutils.notebook.run(f'./0.write_master_data_to_SQL', JOB_TIMEOUT_SECONDS)

# 1.2 Update reference data
print('1.2. Update reference data..')
dbutils.notebook.run(f'./0.file_type_eda_modular', JOB_TIMEOUT_SECONDS)

# 1.3 file type check
print('1.3. Update file type..')
dbutils.notebook.run(f'./0.sql_file_type_check', JOB_TIMEOUT_SECONDS)

# 2. Handle input files
print('2. Handle input files..')
config_azure_auth()
input_files = read_blob_container_folder_abfss(BLOB_CONTAINER_FOLDER_INPUT_ABFSS)
print('Input files',(len(input_files)))

fpr_files = [f for f in input_files if 'fpr' in f['file_path']]
other_files = [f for f in input_files if 'fpr' not in f['file_path']]

# 2.2 Handle FPR files
print('2.2. Handle FPR files..')

for f in fpr_files:
    try:
        print('Calling ' + f['file_path'])
        result = dbutils.notebook.run(f'./1.fpr_preprocess', JOB_TIMEOUT_SECONDS, {'FileUrl': f['file_path']})
        print(result)
    except Exception as e:
        print(e)
        pass

# 2.3 Handle the input files, except FPR files
print('2.3. Handle the input files, except FPR files..')

for f in other_files:
    try:
        print('Calling ' + f['file_path'])
        result = dbutils.notebook.run(f'./2.validate_and_remove_identifier', JOB_TIMEOUT_SECONDS, {'FileUrl': f['file_path'],'FolderName': f['folder_name']})
        print(result)
    except Exception as e:
        print(e)
        pass

# 2.3.1 Processing transform TSS files and update chris and chapr table
print('3. Processing transform TSS files and update chris and chapr table..')
dbutils.notebook.run(f'./5.run_parallel_step_3_and_4', JOB_TIMEOUT_SECONDS)

# 3. Create data view in SQL Server
# print('3. Create data view in SQL Server..')
# dbutils.notebook.run(f'./6.update_CHP_CODE_column', JOB_TIMEOUT_SECONDS)