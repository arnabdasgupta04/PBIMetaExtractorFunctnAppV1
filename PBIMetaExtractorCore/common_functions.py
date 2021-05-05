'''

#   ^           _
#  /_\  |\  /| |_
# /   \ | \/ | |_
#          


    Description  : Common functions.
    Last updated    : 3/11/2021
    Deployment      : Terraform
    Updated by   : Thomas Mathew.
    Functions    : 1.  execute_snowflake_sql | Execute Snowflake SQL and return all records as list of tuples.     
                   2.  dict_clean            | Cleans a dictionary data structure by replacing Pythonic "None" with string 'None' to avoid DB insert failures
                   3.  convert_to_hhmiss     | Converts milliseconds to hh:mi:ss format for readability.
                   4.  get_adf_client        | Authenticate ADF and return ADF Client object.
'''
import os
import sys
import logging
import pandas
import snowflake
from .common_variables import *
from snowflake.connector.pandas_tools import write_pandas
#from azure.mgmt.datafactory import DataFactoryManagementClient
from azure.mgmt.powerbiembedded import PowerBIEmbeddedManagementClient
from azure.common.credentials import ServicePrincipalCredentials

# Get the credentials from Application Settings/ Key Vault.
AME_SNW_USERNAME = os.environ["AME_SNW_USERNAME"]
AME_SNW_PASSWORD = os.environ["AME_SNW_PASSWORD"]
AME_SNW_ACCOUNT = os.environ["AME_SNW_ACCOUNT"]
AME_SNW_DATABASE = os.environ["AME_SNW_DATABASE"]
AME_SNW_SCHEMA = os.environ["AME_SNW_SCHEMA"]
AME_SNW_WAREHOUSE = os.environ["AME_SNW_WAREHOUSE"]
CLIENT_ID = os.environ["AME_CLIENT_ID"]
SECRET    = os.environ["AME_SECRET"]
TENANT    = os.environ["AME_TENANT"]
SUBSCRIPTION_ID    = os.environ["AME_SUBSCRIPTION_ID"]


## Common Exception Message# #################################################################################################
def df_dedup(df:object)->object:
    logging.info("Removing duplicate records in a dataframe")
    count_before_dedup = df.count()[0]
    logging.info(f"Count before removing duplicates : {count_before_dedup}")
    df_dedup = df.drop_duplicates()
    count_after_dedup = df_dedup.count()[0]
    logging.info(f"Count after removing duplicates : {count_after_dedup}")
    return df_dedup


## Common Exception Message# #################################################################################################
def get_exception_message(function_name:str,error_message:str)->dict:
    logging.error("Inside function :  get_exception_message")
    status        = 'Exception'
    status_code   = 500
    output_error = {"status" : status, "status_code":status_code,"function_name" : function_name , "message" :  error_message }
    logging.error(output_error)
    return output_error

## Authenticate and get datafactory client object ############################################################################
#def get_adf_client()->object:
    try:

        credentials = ServicePrincipalCredentials(client_id=CLIENT_ID, secret=SECRET, tenant=TENANT)
        adf_client  = DataFactoryManagementClient(credentials, SUBSCRIPTION_ID)
        
        return adf_client
    except Exception as e:
        error_message = str(e)
        function_name = sys._getframe().f_code.co_name
        output_error = get_exception_message(function_name ,error_message)
        return output_error

## Authenticate and get powerBI client object ############################################################################
def get_pbi_client()->object:
    try:

        credentials = ServicePrincipalCredentials(client_id=CLIENT_ID, secret=SECRET, tenant=TENANT)
        pbi_client  = PowerBIEmbeddedManagementClient(credentials, subscription_id)
        
        return pbi_client
    except Exception as e:
        error_message = str(e)
        function_name = sys._getframe().f_code.co_name
        output_error = get_exception_message(function_name ,error_message)
        return output_error
## Execute Snowflake SQL #####################################################################################################
def execute_snowflake_sql(sql:str)->list:
    try:
        ctx = snowflake.connector.connect(

            user        = AME_SNW_USERNAME,
            password    = AME_SNW_PASSWORD,
            account     = AME_SNW_ACCOUNT,
            database    = AME_SNW_DATABASE,
            schema      = AME_SNW_SCHEMA,
            warehouse   = AME_SNW_WAREHOUSE

            )
        cs = ctx.cursor()
    
        logging.info('Executing SQL : ')
        logging.info(sql)

        ### Execute the Snowflake SQL
        out=cs.execute(sql)
        value = out.fetchall()

        function_name = sys._getframe().f_code.co_name
        status        = 'Success'
        status_code   = 200
        output_success = {"status" : status, "status_code":status_code,"function_name" : function_name , "message" :  value }

        return output_success

    except Exception as e:
        error_message = str(e)
        function_name = sys._getframe().f_code.co_name
        output_error = get_exception_message(function_name ,error_message)
        return output_error
     
    finally:
        ctx.close()
     

## Write to Snowflake ##################################################################################################
def write_to_snowflake(df:object,table_name:str)->int:
    try:
      
        ctx = snowflake.connector.connect(

            user        = AME_SNW_USERNAME,
            password    = AME_SNW_PASSWORD,
            account     = AME_SNW_ACCOUNT,
            database    = AME_SNW_DATABASE,
            schema      = AME_SNW_SCHEMA,
            warehouse   = AME_SNW_WAREHOUSE

            )
        df_count = 0
        df_count = df.count()[0]
        logging.info(f'Appending {df_count} records into {table_name}')
        success, nchunks, nrows, _ = write_pandas(ctx, df, table_name)
        logging.info(f"Impacted rows : {nrows}")    
        return nrows # Return impacted rows
    except Exception as e:
        error_message = str(e)
        function_name = sys._getframe().f_code.co_name
        output_error = get_exception_message(function_name ,error_message)
        return output_error

    logging.info("Closing Snowflake connection.")
    ctx.close()

## Dictionary cleaner #####################################################################################################
def dict_clean(items):
    try:
        result = {}
        for key, value in items:
            if value is None:
                value = 'None'
            result[key] = value
        return result
    except Exception as e:
        error_message = str(e)
        function_name = sys._getframe().f_code.co_name
        output_error = get_exception_message(function_name ,error_message)
        return output_error

## Convert milliseconds to hh:mi:ss format #################################################################################
def convert_to_hhmiss(milliseconds):
    try:
        if milliseconds is None:
            milliseconds=0
        seconds=milliseconds/1000
        min,sec = divmod(seconds, 60)
        hr,min = divmod(min, 60)
        return "%02d:%02d:%02d" % (hr, min, sec)
    except Exception as e:
        error_message = str(e)
        function_name = sys._getframe().f_code.co_name
        output_error = get_exception_message(function_name ,error_message)
        return output_error