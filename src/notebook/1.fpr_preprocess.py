# Databricks notebook source
'''
Purpose: process fpr files
'''


import os
os.chdir('/Workspace/src')
import time
from source.fpr import *
from source.shared import *
from source.configuration import *

# start_time = time.time()
config_azure_auth()

sheet_list = ["'Consolidated Business Analysis'!A10", "'Ratios'!A14","'Segmented Business Analysis'!A10:P142","'Development and Financing'!A11:L58"]
MASTER_FILE = "abfss://chia-inputdata@chiauatdatastorage.dfs.core.windows.net/processed/FPR Master 2020 New Structure.csv"
REG_NOT_NUMBER = r"[^z0-9 ]"
REG_NUMBER = r"\d+"

FILE_URL = dbutils.widgets.get("FileUrl")
# FILE_URL = "abfss://chia-inputdata@chiaproddatastorage.dfs.core.windows.net/input/CHIA/fpr-20230719122432-ch10-2020-T1.xlsx"
print(f'Calling {FILE_URL}')
FILE_NAME = FILE_URL.split('/')[-1] # fpr-20230711022742-ch10-2021-T1.xlsx
file_name_list = FILE_NAME.split('-') 

fpr_df = None
fpr_segmented_df = None
fpr_development_df = None
column_master = master_fpr_column(MASTER_FILE)
file_name_list = FILE_NAME.split('-') # [fpr, 20230711022742, ch10, 2021, T1.xlsx]
year = file_name_list[-2]
chp_id = file_name_list[-3].upper()
flag = True

for sheet_address in sheet_list:
    df_sheet, first_col = fpr_read(FILE_URL, sheet_address, chp_id, year)
    
    if df_sheet == None:
        continue
    if first_col == 'Segmented Statements':
        fpr_segmented_df = df_sheet
        continue
    elif first_col == 'DEVELOPMENT AND FINANCING ASSUMPTIONS':
        fpr_development_df = df_sheet
        continue
    if sheet_address in ["'Consolidated Business Analysis'!A10", "'Ratios'!A14"]:
        df_sheet = df_sheet.withColumnRenamed(first_col,'first_col')
        # filter row in master columns
        df_sheet = df_sheet.where(col('first_col').isin(column_master))
        #2.2 fpr_handling     
    if fpr_df == None:
        fpr_df = df_sheet
    else:
        fpr_df = fpr_df.unionByName(df_sheet, allowMissingColumns = True)    
    

if flag == True:    
    #transpose dataframe
    fpr_df = fpr_df.dropDuplicates(['first_col'])
    fpr_df = tranpose_df(fpr_df,'first_col')

    # Separate text and number in the first column. For example, with the value'Forecast2021' in the first column -> 'Forecast' will be in Metric column and'2021' will be in Year column
    fpr_df = fpr_df.withColumn('Year', regexp_replace(col('first_col'),REG_NOT_NUMBER, ""))
    fpr_df = fpr_df.withColumn('Metric', regexp_replace(col('first_col'),REG_NUMBER, ""))

    # Remove the 'first_col' column
    fpr_df = fpr_df.drop('first_col')
    

    # add info columns
    fpr_df = populate_info_columns(fpr_df, FILE_URL, chp_id, year)
    # display(fpr_df)

    # create new column if have not contain master column
    for column in column_master:
        if column not in fpr_df.columns:
            fpr_df = fpr_df.withColumn('{}'.format(column),lit(0))

    #convert data type
    categorical_columns = ['CHP Name', 'CHP CODE', 'FPR Tier', 'ABN', 'Primary Jurisdiction', 'Metric', 'Reporting year(Financial year end)']
    numeric_columns = [col for col in fpr_df.columns if col not in categorical_columns]
    ratios_columns = ['Working Capital Ratio','Amended Quick Ratio','Operating cash flow adequacy','Gearing Ratio','Interest cover','Return on assets','Cash cost of capital']
    fpr_df = convert_to_decimal(fpr_df,numeric_columns,ratios_columns)

    # display(fpr_df)
    #read loan table
    loan_df = read_loan_table(FILE_URL, chp_id, year)
    display(loan_df)

    #read financiers table
    financiers_df = read_financiers_table(FILE_URL, chp_id, year)
    # display(financiers_df)

    # display(fpr_segmented_df)
    # display(fpr_development_df)
    # write fpr table to SQL Server
    # fpr_table = read_table_from_database(SQL_SERVER_URL,'fpr')
    overwrite_table_scd_type_2(fpr_df, ['Metric','CHP CODE','Year'], SQL_CHIA_UAT_DATABASE,'fpr')
    # print("--- Overwirte fpr table:  {} seconds ---".format(time.time() - start_time))

    # write financiers table to SQL
    # fpr_financiers = read_table_from_database(SQL_SERVER_URL,'fpr_financiers')
    overwrite_table_scd_type_2(financiers_df, ['Metric','CHP CODE','Year'], SQL_CHIA_UAT_DATABASE,'fpr_financiers')
    # print("--- Overwirte fpr_financiers table:  {} seconds ---".format(time.time() - start_time))
    
    # write loans table to SQL
    # fpr_loans = read_table_from_database(SQL_SERVER_URL,'fpr_loans')
    overwrite_table_scd_type_2(loan_df, ['Metric','CHP CODE','Year'], SQL_CHIA_UAT_DATABASE,'fpr_loans')
    # print("--- Overwirte fpr_loans table:  {} seconds ---".format(time.time() - start_time))
    
    # write fpr_segmented table to SQL
    # fpr_segmented = read_table_from_database(SQL_SERVER_URL,'fpr_segmented')
    overwrite_table_scd_type_2(fpr_segmented_df, ['Type','CHP CODE','Year'], SQL_CHIA_UAT_DATABASE,'fpr_segmented')
    # print("--- Overwirte fpr_segmented table:  {} seconds ---".format(time.time() - start_time))
    
    # write fpr_development table to SQL
    # fpr_development = read_table_from_database(SQL_SERVER_URL,'fpr_development')
    overwrite_table_scd_type_2(fpr_development_df, ['Type','CHP CODE','Year'], SQL_CHIA_UAT_DATABASE,'fpr_development')
    # print("--- Overwirte fpr_development table:  {} seconds ---".format(time.time() - start_time))
    
    #change_status_db(FILE_NAME,SQL_SERVER_NAME,SQL_USERNAME,SQL_PASSWORD)
    change_status_db(FILE_NAME)
    move_file(FILE_URL, FILE_NAME, None, 'backup')
    
    # # print("--- Finish fpr process:  {} seconds ---".format(time.time() - start_time))  
    dbutils.notebook.exit("we're good here")
    # print("we're good here")