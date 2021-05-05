'''
#   ^           _
#  /_\  |\  /| |_
# /   \ | \/ | |_
#        

Name : get_activity_runs
Desc : Get activity runs data from Activity runs API and load to Snowflake
Last updated    : 3/11/2021
Deployment      : Terraform
Last updated by : Thomas Mathew.
'''
import pytz
import sys
from datetime import date,datetime,timedelta
import json
import pandas
import logging
from azure.mgmt.datafactory.models import *
from .common_functions import get_adf_client
from .common_variables import *
from .common_functions import execute_snowflake_sql,get_adf_client,write_to_snowflake,get_exception_message,convert_to_hhmiss,dict_clean,df_dedup

### Entry point.
def get_activity_runs(payload):
    try:
        function_name = sys._getframe().f_code.co_name
        logging.info("Inside get_activity_runs")
        default_api_limit=999  ### ADF Limit is 1000 / min
        impacted_rows=0

        factory_name = payload.get('factory_name')
        rg           = payload.get('resource_group')
        api_limit    = payload.get('api_limit')
        watermark_offset = payload.get('watermark_offset')
        delta_days = watermark_offset

        if api_limit is None:
                api_limit = default_api_limit


        merge_sql=f'''
            MERGE into {AME_SNW_DATABASE}.{AME_SNW_SCHEMA}.{T_ADF_META_ACTIVITY_RUNS}            tgt 
            USING
            ( select distinct * FROM {AME_SNW_DATABASE}.{AME_SNW_SCHEMA}.{T_ADF_META_ACTIVITY_RUNS_STG}  ) src 
            ON tgt.ACTIVITY_RUN_ID=src.ACTIVITY_RUN_ID and src.PIPELINE_RUN_ID = tgt.PIPELINE_RUN_ID
                
            WHEN  matched THEN UPDATE SET
            tgt.ADDITIONAL_PROPERTIES=src.ADDITIONAL_PROPERTIES,
            tgt.PIPELINE_NAME=src.PIPELINE_NAME,
            tgt.PIPELINE_RUN_ID=src.PIPELINE_RUN_ID,
            tgt.ACTIVITY_NAME=src.ACTIVITY_NAME,
            tgt.ACTIVITY_TYPE=src.ACTIVITY_TYPE,
            tgt.ACTIVITY_RUN_ID=src.ACTIVITY_RUN_ID ,
            tgt.LINKED_SERVICE_NAME=src.LINKED_SERVICE_NAME ,
            tgt.STATUS=src.STATUS ,
            tgt.ACTIVITY_RUN_START=src.ACTIVITY_RUN_START,
            tgt.ACTIVITY_RUN_END=src.ACTIVITY_RUN_END ,
            tgt.DURATION_IN_MS=src.DURATION_IN_MS ,
            tgt.DURATION_HH_MI_SS=src.DURATION_HH_MI_SS ,
            tgt.INPUT=src.INPUT ,
            tgt.OUTPUT=src.OUTPUT ,
            tgt.ERROR=src.ERROR ,
            tgt.ETL_UPDATE_TS=to_timestamp_ntz(convert_timezone('UTC', current_timestamp()))
            
            WHEN NOT matched THEN INSERT VALUES(
            src.ADDITIONAL_PROPERTIES,
            src.PIPELINE_NAME,
            src.PIPELINE_RUN_ID,
            src.ACTIVITY_NAME,
            src.ACTIVITY_TYPE,
            src.ACTIVITY_RUN_ID ,
            src.LINKED_SERVICE_NAME ,
            src.STATUS ,
            src.ACTIVITY_RUN_START ,
            src.ACTIVITY_RUN_END ,
            src.DURATION_IN_MS ,
            src.DURATION_HH_MI_SS ,
            src.INPUT ,
            src.OUTPUT ,
            src.ERROR ,
            src.ETL_INSERT_TS ,
            src.ETL_UPDATE_TS ,
            src.ETL_INSERT_ID ,
            src.ETL_UPDATE_ID )
        '''



        adf_client   = get_adf_client()

        ######### GET DATE FILTER
        tz = pytz.timezone('UTC')
        execution_result = execute_snowflake_sql(f"select nvl(max(ETL_UPDATE_TS),to_timestamp_ntz(convert_timezone('UTC', current_timestamp()))) as previous_datetime from {AME_SNW_DATABASE}.{AME_SNW_SCHEMA}.{T_ADF_META_ACTIVITY_RUNS}")
        sql_exec_status_code = execution_result['status_code']
        if sql_exec_status_code != 200 :
            logging.error('Exception in execute_snowflake_sql. Stopping activity execution.')
            return execution_result
        max_date_obj = execution_result['message']
        max_date = pytz.utc.localize(max_date_obj[0][0]) 
        #max_date = max_date_obj[0][0] ####  ## Include timezone info to get rid msrestazure tz warning.
        
        previous_time = max_date -  timedelta(days=delta_days)
        current_time = datetime.datetime.now(tz) + timedelta(days=0)

        logging.info(f"Current time : {current_time} | Previous Time : {previous_time}")
        ########

        ### Acitivty Runs are obtained by passing pipeline run id . There is an API-LIMIT of 1000/min at server side and the below SQL will process a chunk of "500" (a default API limit) pipeline run-ids in the first come first serve fashion.

        pp_extract_sql=f'''

        select  p.run_id AS RUN_ID from {AME_SNW_DATABASE}.{AME_SNW_SCHEMA}.{T_ADF_META_PIPELINE_RUNS}   p  left outer join {AME_SNW_DATABASE}.{AME_SNW_SCHEMA}.{T_ADF_META_ACTIVITY_RUNS}  a
        on p.run_id = a.pipeline_run_id where a.pipeline_run_id is null or a.status='InProgress' 
        group by  p.run_id,P.ETL_UPDATE_TS
        order by  P.ETL_UPDATE_TS ASC LIMIT {api_limit}

        '''
        logging.info(pp_extract_sql)
        execution_result = execute_snowflake_sql(pp_extract_sql)
        sql_exec_status_code = execution_result['status_code']
        if sql_exec_status_code != 200 :
            logging.error('Exception in execute_snowflake_sql. Stopping activity execution.')
            return execution_result
        pipeline_runids = execution_result['message']
        activity_generator_list = []
        for tuples in pipeline_runids:
            pp_run_id =  tuples[0] # select the element within tuple.
            activity_generator = get_activity_detail(adf_client,rg,factory_name,pp_run_id,previous_time,current_time)
            activity_generator_list.append(activity_generator)
            
        gen_activity_runs = generate_activity(activity_generator_list)
        logging.info("Started fetching the Activity API..")
        df_activity_runs = pandas.DataFrame(data=gen_activity_runs,columns=COLUMNS_T_ADF_META_ACTIVITY_RUNS)
        #df_activity_runs = df_dedup(df_activity_runs)
        count_of_df = df_activity_runs.count()[0]
       
        logging.info(f"Total count of records : {count_of_df}")

        if count_of_df!=0:
            ##################### ACTIVITY SNOWFLAKE LOAD
            ###Truncate
            logging.info(f'Truncating {AME_SNW_DATABASE}.{AME_SNW_SCHEMA}.{T_ADF_META_ACTIVITY_RUNS_STG}')
            execution_result = execute_snowflake_sql(f"TRUNCATE TABLE {AME_SNW_DATABASE}.{AME_SNW_SCHEMA}.{T_ADF_META_ACTIVITY_RUNS_STG}")
            sql_exec_status_code = execution_result['status_code']
            if sql_exec_status_code != 200 :
                logging.warn('Exception in execute_snowflake_sql. Stopping activity execution.')
                return execution_result 
            ###Load
            logging.info(f'Loading {AME_SNW_DATABASE}.{AME_SNW_SCHEMA}.{T_ADF_META_ACTIVITY_RUNS_STG}')
            impacted_rows = write_to_snowflake(df_activity_runs,T_ADF_META_ACTIVITY_RUNS_STG)
            ##Update  variant
            #logging.info(f"Impacted rows : {impacted_rows}")
            logging.info(f'Updating variant columns in {AME_SNW_DATABASE}.{AME_SNW_SCHEMA}.{T_ADF_META_ACTIVITY_RUNS_STG}')
            sql=f'''update {AME_SNW_DATABASE}.{AME_SNW_SCHEMA}.{T_ADF_META_ACTIVITY_RUNS_STG} set ADDITIONAL_PROPERTIES=parse_json(ADDITIONAL_PROPERTIES),
                                    INPUT=parse_json(INPUT),
                                    OUTPUT=parse_json(OUTPUT),
                                    ERROR = parse_json(ERROR)
                                '''
            execution_result=execute_snowflake_sql(sql)
            sql_exec_status_code = execution_result['status_code']
            if sql_exec_status_code != 200 :
                logging.error('Exception in execute_snowflake_sql. Stopping activity execution.')
                return execution_result
            updated_rows = execution_result['message'][0][0]
            logging.info(f'Updated rows {updated_rows}')

            ###Merge
            logging.info(f'Merge to {AME_SNW_DATABASE}.{AME_SNW_SCHEMA}.{T_ADF_META_ACTIVITY_RUNS}')
            execution_result=execute_snowflake_sql(merge_sql)
            sql_exec_status_code = execution_result['status_code']
            if sql_exec_status_code != 200 :
                logging.error('Exception in execute_snowflake_sql. Stopping activity execution.')
                return execution_result
            impacted_rows =execution_result['message'][0][0]
            logging.info(f"Impacted rows after merge : {impacted_rows}")
            logging.info("Completed successfully")
        else:
            logging.info("No records to load")
            logging.info("Completed successfully")
        
        status='Success'
        message={f"Impacted rows on  {AME_SNW_DATABASE}.{AME_SNW_SCHEMA}.{T_ADF_META_ACTIVITY_RUNS}":impacted_rows}
        output_success = {"status" : status, "status_code":200,"function_name" : function_name , "message" :  message }
        return output_success
    except Exception as e:
        error_message = str(e)
        function_name = sys._getframe().f_code.co_name
        output_error = get_exception_message(function_name ,error_message)
        return output_error




 ## Yeild a single generator with the dictionary of triggers.
def generate_activity(activity_generator_list:list)->object:
    for gen in activity_generator_list:
        for element in gen:
            yield element


def get_activity_detail(adf_client,rg,factory_name,runid:str,previous_time:object,current_time:object)->dict:
    try:
        filter_params = RunFilterParameters(last_updated_after=previous_time , last_updated_before=current_time)
        activity_runs=adf_client.activity_runs.query_by_pipeline_run(rg,factory_name,runid,filter_params)

        for run in activity_runs.value:
            additional_properties = run.additional_properties
            pipeline_name = run.pipeline_name
            pipeline_run_id = run.pipeline_run_id
            activity_name = run.activity_name
            activity_type = run.activity_type
            activity_run_id = run.activity_run_id
            linked_service_name = run.linked_service_name
            status = run.status
            activity_run_start = run.activity_run_start
            activity_run_end = run.activity_run_end
            duration_in_ms = run.duration_in_ms
            input = run.input
            output = run.output
            error = run.error


            if activity_run_start is not None:
                activity_run_start=activity_run_start.isoformat()
            else:
                activity_run_start='1900-01-01 00:00:00.000'
            if activity_run_end is not None:
                activity_run_end=activity_run_end.isoformat()
            else:
                activity_run_end='2999-01-01 00:00:00.000'

            derived_duration = convert_to_hhmiss(duration_in_ms)

            ## Cleaning the dictionaries by replacing None with 'None' to avoid database failures. None is not
            ## recognized in Snowflake.
            dict_str = json.dumps(additional_properties)
            additional_properties_clean = json.loads(dict_str, object_pairs_hook=dict_clean)
            if additional_properties_clean=='None' or additional_properties_clean is None:
                additional_properties_clean={}

            dict_str = json.dumps(input)
            input_clean = json.loads(dict_str, object_pairs_hook=dict_clean)
            if input_clean=='None' or input_clean is None:
                input_clean={}
            
            dict_str = json.dumps(output)
            output_clean = json.loads(dict_str, object_pairs_hook=dict_clean)
            if output_clean=='None' or output_clean is None:
                output_clean={}

            dict_str = json.dumps(error)
            output_error = json.loads(dict_str, object_pairs_hook=dict_clean)
            if output_error=='None' or output_error is None:
                output_error={}

            output = {

                'ADDITIONAL_PROPERTIES' : str(additional_properties_clean),
                'PIPELINE_NAME' : pipeline_name,
                'PIPELINE_RUN_ID' : pipeline_run_id,
                'ACTIVITY_NAME' : activity_name   ,
                'ACTIVITY_TYPE' : activity_type,
                'ACTIVITY_RUN_ID':activity_run_id,
                'LINKED_SERVICE_NAME':linked_service_name,
                'STATUS':status,
                'ACTIVITY_RUN_START':activity_run_start,
                'ACTIVITY_RUN_END':activity_run_end,
                'DURATION_IN_MS':duration_in_ms,
                'DURATION_HH_MI_SS':derived_duration,
                'INPUT':str(input_clean),
                'OUTPUT':str(output_clean),
                'ERROR':str(error),
                'ETL_INSERT_TS' : ETL_TIME,
                'ETL_UPDATE_TS' : ETL_TIME,
                'ETL_INSERT_ID':ETL_ID,
                'ETL_UPDATE_ID':ETL_ID
            
            }
            yield output
    except Exception as e:
        error_message = str(e)
        function_name = sys._getframe().f_code.co_name
        output_error = get_exception_message(function_name ,error_message)
        yield output_error


