'''
#   ^           _
#  /_\  |\  /| |_
# /   \ | \/ | |_
#        

Function        : get_datasets
Description     : Get datasets data from DataSets  API and load to Snowflake
Last updated    : 3/11/2021
Deployment      : Terraform
Last updated by : Thomas Mathew.
'''

import sys
import json
import pandas
import logging
from .common_functions import get_adf_client
from .common_variables import *
from .common_functions import execute_snowflake_sql,get_adf_client,write_to_snowflake,get_exception_message,df_dedup



## Entry Point
def get_datasets(payload:dict)->dict:

    try : 
        function_name = sys._getframe().f_code.co_name
        logging.info(f"Inside {function_name}")

        factory_name = payload.get('factory_name')
        rg           = payload.get('resource_group')
        api_limit    = payload.get('api_limit')
    

        dsobjlist = []
        default_api_limit = 500
        page_count = 1
        status = 'Success'

        if api_limit is None:
            api_limit = default_api_limit
   
        adf_client   = get_adf_client()

        dsobj           = adf_client.datasets.list_by_factory(rg,factory_name)._get_next()
        dsobj_json      = dsobj.json()
        nextLink        = dsobj_json['nextLink'] # For pagination
        dsvalue         = dsobj_json['value']
        dsobjlist.append(dsvalue)  # Get first element
        
        

        logging.info("Invoking ADF Datasets API..") # Paginate
        while 'nextLink' in dsobj_json and page_count < api_limit:
            dsobj = adf_client.datasets.list_by_factory(rg,factory_name)._get_next(nextLink)
            dsobj_json=dsobj.json()
            dsvalue=dsobj_json['value']
            dsobjlist.append(dsvalue)
            if 'nextLink' in dsobj_json:
                nextLink=dsobj_json['nextLink']
            page_count+=1
        logging.info(f"Total pages scanned : {page_count}")

        gen_ds_list = parse_ds_object(dsobjlist)

        # Create pandas dataframe
        df = pandas.DataFrame(data = gen_ds_list, columns = COLUMNS_T_ADF_META_DATASETS)
        #df = df_dedup(df)
        ### Truncate
        sql = f"TRUNCATE TABLE {AME_SNW_DATABASE}.{AME_SNW_SCHEMA}.{T_ADF_META_DATASETS}"
        execution_result = execute_snowflake_sql(sql)
        sql_exec_status_code = execution_result['status_code']
        if sql_exec_status_code != 200 :
            logging.error('Exception in execute_snowflake_sql. Stopping activity execution.')
            return execution_result
        max_date_obj = execution_result['message']

        ### Load
        impacted_rows = write_to_snowflake(df,T_ADF_META_DATASETS)

        ### Update
        sql = f"update {AME_SNW_DATABASE}.{AME_SNW_SCHEMA}.{T_ADF_META_DATASETS} set PROPERTIES=parse_json(PROPERTIES)"
        execution_result = execute_snowflake_sql(sql)
        sql_exec_status_code = execution_result['status_code']
        if sql_exec_status_code != 200 :
            logging.error('Exception in execute_snowflake_sql. Stopping activity execution.')
            return execution_result

        logging.info("Pipeline Runs load complete")
        logging.info('Clossing the connection..')

        adf_client.close()
        message = f"Impacted rows on {T_ADF_META_DATASETS} : {impacted_rows}"
        output_success = {"status" : status, "status_code":200,"function_name" : function_name , "message" :  message }
        return output_success
    
    except Exception as e:
        error_message   = str(e)
        function_name   = sys._getframe().f_code.co_name
        output_error    = get_exception_message(function_name ,error_message)
        return output_error
## END 

# Transformation Logic
def parse_ds_object(ds_obj_list:list)->dict:
    try:
        for i in ds_obj_list:
            for j in i:
                id            = j['id']
                name          = j['name']
                type          = j['type']
                properties    = json.dumps(j['properties'])
                etag          = j['etag']
                output = {
                                'ID'        :id,
                                'NAME'      :name,
                                'TYPE'      :type,
                                'PROPERTIES':properties,
                                'ETAG'      :etag,
                                'ETL_INSERT_TS' : ETL_TIME,
                                'ETL_UPDATE_TS' : ETL_TIME,
                                'ETL_INSERT_ID' :ETL_ID,
                                'ETL_UPDATE_ID' :ETL_ID
                            }
                yield output

    except Exception as e:
        error_message = str(e)
        function_name = sys._getframe().f_code.co_name
        output_error = get_exception_message(function_name ,error_message)
        yield output_error