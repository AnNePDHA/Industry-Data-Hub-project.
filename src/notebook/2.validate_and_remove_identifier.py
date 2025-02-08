# Databricks notebook source
'''
Purpose: validate and clean data
'''



import os
os.chdir('/Workspace/src')

from source.configuration import *
from source.shared import *

FILE_URL = dbutils.widgets.get("FileUrl")
FOLDER_NAME = dbutils.widgets.get("FolderName")
# FILE_URL = "abfss://chia-inputdata@chiaproddatastorage.dfs.core.windows.net/input/CHIA/chapr-20230710050000-T1.xlsx"
# FOLDER_NAME = "CHIA/"
validate_POSTCODE = ["chapr"]
FILE_NAME, FILE_TYPE, FILE_FORMAT, CHP_ID, DATE_LOADED, df_base = read_data(FILE_URL,FOLDER_NAME)

# print('FILE_NAME',FILE_NAME)
# print('FILE_TYPE',FILE_TYPE)
# print('CHP_ID',CHP_ID)
# print('DATE_LOADED',DATE_LOADED)

# Read 'file_type' table to 'rule_df' dataframe
file_type_df = read_table_from_database(SQL_chia_uat_info_SERVER_URL,'file_type')
rule_df = file_type_df.select('*').where((lower(col('FileType')) == FILE_TYPE) & (col('Active') == True) & (col('current_id') == True) & (col('UseInValidation') == True))

# display(df_base)


if 'tenantsatisfactionsurvey' in FILE_TYPE:
    # Remove first row in 'df' dataframe
    df_base = spark.createDataFrame(df_base.tail(df_base.count()-1), df_base.schema) 
elif 'chapr' in FILE_TYPE:
    df_base = check_alter_names('chapr', file_type_df,df_base)
    df_base = df_base.toDF(*(c.lower().replace(" ", "") for c in df_base.columns))

df = df_base

# Remove " in column names
df = df.toDF(*(c.replace('"', "") for c in df.columns))
display(df)


# replace dot character to comma character in 'ColumnName' column
# rule_df = rule_df.withColumn('ColumnName', regexp_replace(col('ColumnName'),r"[.]",","))

error_df = None
display(rule_df)
BusinessKey = get_business_key_column(rule_df)
print(BusinessKey)

if all(key in df.columns for key in BusinessKey):
    if df.count() > df.dropDuplicates(BusinessKey).count():
        error_df = insert_an_error_row(error_df, FILE_NAME, FILE_TYPE, CHP_ID, DATE_LOADED, 'Transformation error','Duplicated data having the same Business Key' , None, None, None)
else:
    error_df = insert_an_error_row(error_df, FILE_NAME, FILE_TYPE, CHP_ID, DATE_LOADED, 'Transformation error','Do not have Business Key column' , None, None, None)

if check_column(df,rule_df) == False:
    # write an error row to error_logging table
    error_df = insert_an_error_row(error_df, FILE_NAME, FILE_TYPE, CHP_ID, DATE_LOADED,'Ingestion error','The file contains invalid columns',None,BusinessKey,None)

    # stop the process and notify in Main notebook
    dbutils.notebook.exit('Error: The file contains invalid columns')
    # print('Error: The file contains invalid columns')

else:
    for row in rule_df.rdd.collect():
        # print("Column: {} - PrivateIdentifier: {} - CanBeNull: {} - BusinessKey: {}".format(row.ColumnName, row.PrivateIdentifier, row.CanBeNull, row.BusinessKey))
        if(row.ColumnName in df.columns) == False:
            continue
        
        if row.PrivateIdentifier == True:
            df = remove_private_identifier(df,row.ColumnName,row.DataType,row.CanBeNull)
            continue
        if row.CanBeNull == False:
            # print("Column: ",row.ColumnName)
            error_df= validation_blank(df, row.ColumnName, BusinessKey, error_df, FILE_NAME,FILE_TYPE,CHP_ID, DATE_LOADED)  
        
        # check data type
        error_df = validation_dataType(df,row.ColumnName,row.DataType,BusinessKey, error_df, FILE_NAME,FILE_TYPE,CHP_ID, DATE_LOADED)

        # check reference
        if row.ReferenceData != None:
            error_df = validate_reference(df,row.ColumnName,BusinessKey,row.ReferenceData, error_df, FILE_NAME,FILE_TYPE,CHP_ID, DATE_LOADED)

# Check business key in df or not
# has_business_key, error_df = check_business_key_column(df, BusinessKey, error_df, FILE_NAME, FILE_TYPE, CHP_ID, DATE_LOADED)

# Check duplicate

# Check POSTCODE column in df or not
if FILE_TYPE in validate_POSTCODE:
    df_validate_postcode = df.toDF(*(c.lower() for c in df.columns))
    if "postcode" not in df_validate_postcode.columns:
        error_df = insert_an_error_row(error_df, FILE_NAME, FILE_TYPE, CHP_ID, DATE_LOADED, 'Transformation error', 'Do not have postcode column',None, None,None)
    # else:
    #     if df_validate_postcode.filter(col("postcode").isNull()).count() != 0:
    #         error_df = insert_an_error_row(error_df, FILE_NAME, FILE_TYPE, CHP_ID, DATE_LOADED, 'Transformation error', 'Having null value in postcode column',None, None,None)

# df = df.toDF(*(c.replace(',', '.') for c in df.columns))

if 'tenantsatisfactionsurvey' in FILE_TYPE:
    temp = spark.createDataFrame([df_base.collect()[0]], df_base.schema)
    final_df = temp.union(df)
else:
    final_df = df
    
if error_df != None:
    count_errors = error_df.where((col('FileName') == FILE_NAME) & (col('DateLoaded') == DATE_LOADED)).count()
    error_logging = read_table_from_database(SQL_chia_uat_info_SERVER_URL,'error_logging')
    
    # error_logging = overwrite_table(error_df, error_logging, ['FileName', 'DateLoaded', 'ColumnName', 'ErrorType', 'ErrorMessage', 'RowNumber'], SQL_chia_prod_info_SERVER_URL,'error_logging')
    overwrite_table(error_df, ['FileName', 'DateLoaded', 'ColumnName', 'ErrorType', 'ErrorMessage', 'RowNumber'], SQL_CHIA_UAT_INFO_DATABASE,'error_logging')
    
    # move the file to 'error' folder
    move_file(FILE_URL,FILE_NAME, None, 'error')
    
    # print the amount of file's errors
    dbutils.notebook.exit(f'{FILE_NAME} has {count_errors} error(s)')
    # print("{} has {} error(s)".format(FILE_NAME,count_errors))

else: # if the file has no error
    # export the cleaned dataframe to csv and store in 'proccessed' folder
    temp_file = '/tmp/temp_file.xlsx'
    final_df.toPandas().to_excel(temp_file, engine='openpyxl')
    if (FILE_FORMAT == '.xlsx') or (FILE_FORMAT == '.xls'):
        # copy_file(FILE_URL,FILE_NAME, None, 'processed')
        dbutils.fs.cp(f'file:{temp_file}', f"abfss://{BLOB_CONTAINER}@{BLOB_STORAGE_URL}/processed/CHIA/{FILE_NAME}")
    else:
        export_to_csv(final_df,FILE_NAME, None, 'processed')
        
    # export the cleaned dataframe to csv and store in 'proccessed' folder
    if 'fpr' not in FILE_NAME:
        if (FILE_FORMAT == '.xlsx') or (FILE_FORMAT == '.xls'):
            # copy_file(FILE_URL,FILE_NAME, None, 'write_to_sql')
            dbutils.fs.cp(f'file:{temp_file}', f"abfss://{BLOB_CONTAINER}@{BLOB_STORAGE_URL}/write_to_sql/CHIA/{FILE_NAME}")
        else:
            export_to_csv(final_df, FILE_NAME, None,'write_to_sql')

   # move the raw file from 'input' folder to 'back_up' folder
    move_file(FILE_URL,FILE_NAME, None, 'backup')
        
    # Change file's status in database
    # change_status_db(FILE_NAME,SQL_SERVER_NAME,SQL_USERNAME,SQL_PASSWORD)
    change_status_db(FILE_NAME)
    dbutils.notebook.exit("we're good here")
    # print("we're good here")