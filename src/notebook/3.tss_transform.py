# Databricks notebook source
'''
Purpose: process tss files
'''


import os
os.chdir('/Workspace/src')
from source.shared import *
from source.tss import *
import copy

######################################################################################################
config_azure_auth()
processed_files = read_blob_container_folder_abfss(BLOB_CONTAINER_FOLDER_WRITE_TO_SQL_ABFSS)
name_rules, duplicated_rules = read_rule(FILE_RULE_TSS_URL)
PRIMARY_KEY = ['ID.case']
# error_files = []
# for k, v in name_rules.items():
#     print("{} : {}".format(k, v))
# print(name_rules)
for f in processed_files:
    if 'tenantsatisfactionsurvey' in f['file_path']:
        FILE_URL = f['file_path'] #                
     
        FILE_NAME = FILE_URL.split('/')[-1] # tenantsatisfactionsurvey-20230710080634-T2.xlsx
        FOLDER_NAME = f['folder_name']
        FILE_TYPE = 'tenantsatisfactionsurvey'
        CHP_ID = None
        DATE_LOADED = None
        print(f'Calling {FILE_URL}')
        try:
            FILE_NAME, FILE_TYPE, FILE_FORMAT, CHP_ID, DATE_LOADED,df = read_data(FILE_URL, FOLDER_NAME)
            
            df = df.toDF(*(c.replace('.', '') for c in df.columns))
            list_header = []
            list_question = []
            error_df = None

            for ele in df.columns:
                list_header.append(ele.lower())
            for ele in df.collect()[0]:
                if type(ele) is str:
                    list_question.append(ele.lower())
                else:
                    list_question.append("")
            
            
            selected_columns = transform_rule(list_header, list_question, name_rules)

            # Check POSTCODE column
            if "POSTCODE" not in selected_columns.values():
                error_df = insert_an_error_row( error_df, FILE_NAME, FILE_TYPE, CHP_ID, datetime.now(), 'Transformation error', 'Do not have postcode column', None, "ID.case", None)

            # Check column having the same SQL name
            duplicated_columns = check_duplicated_sql_name(selected_columns)

            if len(duplicated_columns) == 0 and error_df == None:
                # Change header name to sql name
                headers = list(selected_columns.keys())
                df = df.select(*headers)
                schema_list = []
                for col in df.columns:
                    schema_list.append(StructField(col, StringType(), True))
                schema = StructType(schema_list)
                df = spark.createDataFrame(df.tail(df.count() - 1),schema)
                for header, sql_name in selected_columns.items():
                    df = df.withColumnRenamed(header, sql_name)

                # Calculate PWI_OVERALL
                df = overall_pwi(df)  

                # Get CHP Real_Name, Tier from CHRIS and CHP table
                chris_df = read_table_from_database(SQL_SERVER_URL,'chris') # Read CHRIS table
                chp_df = read_table_from_database(SQL_SERVER_URL,'CHP') # Read CHP table

                # Select CHP CODE, CHRIS Tier with the latest year
                df1 = chris_df.select(["YEAR", "CHP CODE", "CHRIS Tier"])
                df2 = df1.groupBy("CHP CODE").agg(max("YEAR"))
                df1 = df1.join(df2, (df1["CHP CODE"] == df2["CHP CODE"]) & (df1["YEAR"] == df2["max(YEAR)"]), "left").drop(df2["CHP CODE"])
                df1 = df1.distinct()

                # Join df1 with CHP table to get CHP Name
                df = df.join(df1, ["CHP CODE"], 'left').drop(df1["YEAR"])
                df = df.drop("max(YEAR)")

                # Check POSTCODE column
                # Read Postcode LGA and SED Unique table
                lga_df = read_table_from_database(SQL_SERVER_URL,'CG_POSTCODE_LGA_UNIQUE') 
                sed_df = read_table_from_database(SQL_SERVER_URL,'CG_POSTCODE_SED_UNIQUE') 

                # Join df with lga_df to get LGA_CODE and LGA_NAME
                df = df.join(lga_df, ["POSTCODE"], 'left').drop("RATIO_FROM_TO", "INDIV_TO_REGION_QLTY_INDICATOR", "OVERALL_QUALITY_INDICATOR", "BMOS_NULL_FLAG")

                # Join df with sed_df to get SED_CODE and SED_NAME
                df = df.join(sed_df, ["POSTCODE"], 'left').drop("RATIO_FROM_TO", "INDIV_TO_REGION_QLTY_INDICATOR", "OVERALL_QUALITY_INDICATOR", "BMOS_NULL_FLAG")
                
                # Change name columns
                df = change_name_columns(df, ["CHRIS Tier"], ["TIER"])
                
                # Duplicate columns
                for SQL_name, columns_dup in duplicated_rules.items():
                    if SQL_name in df.columns:
                        for cols in columns_dup:
                            df = df.withColumn(cols, df[SQL_name])

                display(df)

                
                # Read table from SQL Server
                # tss_df = read_table_from_database(SQL_SERVER_URL,'tss')
                overwrite_table(df, PRIMARY_KEY, SQL_CHIA_UAT_DATABASE,'tss', 1)

                # Delete file
                delete_file(FILE_NAME, None, 'write_to_sql')
                
                
            elif len(duplicated_columns) != 0:
                for header in duplicated_columns:
                    question = df.select(header).first()[0]
                    if type(question) is not str:
                        question = ""
                    print("{} : {}".format(header, selected_columns[header]))
                    error_df = insert_an_error_row(error_df, FILE_NAME, FILE_TYPE, CHP_ID, datetime.now(), 'Transformation error', 'questions ' + question + ' have the same column name after transforming', selected_columns[header], "ID.case", None)  
                
            if error_df != None:
                overwrite_table(error_df, ['FileName', 'DateLoaded', 'ColumnName', 'ErrorType', 'ErrorMessage', 'RowNumber'], SQL_CHIA_UAT_INFO_DATABASE, 'error_logging')

                # Move file
                move_file(FILE_URL, FILE_NAME, None, 'error')
                
                print(f'{FILE_NAME} has error(s)')
                # error_files += [FILE_NAME]
            else:
                print("we're good here")
        except Exception as e:
            # error_files += [FILE_NAME]
            # move_file(FILE_URL, FILE_NAME, None, 'error')
            print(f'{FILE_NAME} : {e}')
    

# dbutils.notebook.exit(f'There are {len(error_files)} file(s) having error(s) in TSS')