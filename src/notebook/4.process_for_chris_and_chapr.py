# Databricks notebook source
'''
This notebook writes chris, chapr, boscar and similar files's data into SQL.
'''

# Import libraries
import os
os.chdir('/Workspace/src')
from source.shared import *
from source.configuration import *
from source.reference_data import *
import pyodbc

config_azure_auth()

# Get a dictionary of all files in the write_to_sql folder
processed_files = read_blob_container_folder_abfss(BLOB_CONTAINER_FOLDER_WRITE_TO_SQL_ABFSS)
# print(processed_files)
# Get the list of files with column adt_timestamp
files_with_adt_timestamp = ['chris', 'chapr']



if len(processed_files) != 0:
    ''' Get the dictionary of files we can handle from SQL table added_files
    ---------------------------'''


    tokenstruct = get_access_token_for_conn()
    SQL_COPT_SS_ACCESS_TOKEN = 1256
    conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server}; SERVER=chiadwuat.database.windows.net; DATABASE=chia-uat', attrs_before = {SQL_COPT_SS_ACCESS_TOKEN: tokenstruct})    
    cursor = conn.cursor() 

    # Query all the current tables
    sql_query = "SELECT * FROM added_files"
    cursor.execute(sql_query)
    # Fetch the query results as a list of rows
    rows = cursor.fetchall()
    # Close SQL connection
    cursor.close()
    conn.close()
    
    # Convert rows into a dictionary
    added_file_data = {}
    for row in rows:
        added_file_data[row[0]] = {'SQL_table': row[0], 'primary_keys': row[1].split(','), 'numeric_columns': row[2]}
        if added_file_data[row[0]]['numeric_columns'] != None:
            added_file_data[row[0]]['numeric_columns'] = added_file_data[row[0]]['numeric_columns'].split(',')
    # print(added_file_data)
    ''' END Get the dictionary of files we can handle from SQL table added_files
    ---------------------------'''

    ''' Process each file: check if the file is in the dictionary then we write it into SQL
    ---------------------------'''
    for added_file in processed_files:
        # Get the names of the files

        file_name_full = added_file['file_path'].split('/')[-1] # the full file name, such as tenantsatisfactionsurvey-20230515074433-T2.csv
        file_name = file_name_full.split('-')[0].lower() # short file name, such as tenantsatisfactionsurvey

        if file_name in added_file_data:
            print("Loading files into SQL:", file_name_full)
            try:
                FILE_NAME, FILE_TYPE, FILE_FORMAT, CHP_ID, DATE_LOADED, df_csv_file = read_data(added_file['file_path'], added_file['folder_name'])
                
                # display(df_csv_file)
                # if FILE_TYPE == 'chapr':
                #     df_csv_file = df_csv_file.toDF(*(c.lower().replace(" ", "") for c in df_csv_file.columns))
                # Convert type for numeric columns
                
                if "chapr" in file_name.lower():
                    # Read Postcode LGA and SED Unique table
                    lga_df = read_table_from_database(SQL_SERVER_URL,'CG_POSTCODE_LGA_UNIQUE')

                    # Join df with lga_df to get LGA_CODE and LGA_NAME
                    df_csv_file = df_csv_file.join(lga_df, ["POSTCODE"], 'left').drop("RATIO_FROM_TO", "INDIV_TO_REGION_QLTY_INDICATOR", "OVERALL_QUALITY_INDICATOR", "BMOS_NULL_FLAG")
                
                # display(df_csv_file)

                if added_file_data[file_name]['numeric_columns'] != None:
                    for c in added_file_data[file_name]['numeric_columns']:
                        df_csv_file = df_csv_file.withColumn(c, col(f"`{c}`").cast(DecimalType(18,2)))
                
                # Read the current SQL data into a data frame
                # df_sql_table = read_table_from_database(SQL_SERVER_URL,added_file_data[file_name]['SQL_table']) # The data from SQL table


                # Write the data into SQL by SCD type I
                if file_name_full.find('-T1') >= 0:
                    if file_name in files_with_adt_timestamp:
                        overwrite_table(df_csv_file, added_file_data[file_name]['primary_keys'], SQL_CHIA_UAT_DATABASE, added_file_data[file_name]['SQL_table'], 1)
                    else:
                        overwrite_table(df_csv_file, added_file_data[file_name]['primary_keys'], SQL_CHIA_UAT_DATABASE, added_file_data[file_name]['SQL_table'])
                
                # Write the data into SQL by SCD type II
                if file_name_full.find('-T2') >= 0:
                    overwrite_table_scd_type_2(df_csv_file, added_file_data[file_name]['primary_keys'], SQL_CHIA_UAT_DATABASE, added_file_data[file_name]['SQL_table'])

                # Delete the current csv file
                delete_file(FILE_NAME, None, 'write_to_sql')
                print("we're good here")
            except Exception as e:
                # error_files += [FILE_NAME]
                # move_file(added_file['file_path'], file_name_full, None, 'error')
                print(f'{file_name_full} : {e}')
    
        
    # dbutils.notebook.exit("we're good here")       
    ''' END Process each file: check if the file is in the dictionary then we write it into SQL
    ---------------------------'''