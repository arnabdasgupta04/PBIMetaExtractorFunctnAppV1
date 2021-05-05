'''
#   ^           _
#  /_\  |\  /| |_
# /   \ | \/ | |_
#          

    Common variable declaration and value assignment. Get the value from Az Function properties.
'''
import os
import pytz
import datetime

AVAILABLE_API_LIST=['GetPipelines','GetTriggers','GetPipelineRuns','GetActivityRuns','GetTriggerRuns','GetLinkedServices','GetDatasets']

## Key vault entries

AME_SNW_USERNAME        = os.environ["AME_SNW_USERNAME"]
AME_SNW_PASSWORD        = os.environ["AME_SNW_PASSWORD"]
AME_SNW_ACCOUNT         = os.environ["AME_SNW_ACCOUNT"]
AME_SNW_DATABASE        = os.environ["AME_SNW_DATABASE"]
AME_SNW_SCHEMA          = os.environ["AME_SNW_SCHEMA"]
AME_SNW_WAREHOUSE       = os.environ["AME_SNW_WAREHOUSE"]
AME_CLIENT_ID           = os.environ["AME_CLIENT_ID"]
AME_SECRET              = os.environ["AME_SECRET"]
AME_TENANT              = os.environ["AME_TENANT"]
AME_SUBSCRIPTION_ID     = os.environ["AME_SUBSCRIPTION_ID"]

## Table names
T_ADF_META_PIPELINES           = 'T_ADF_META_PIPELINES'
T_ADF_META_PIPELINE_RUNS       = 'T_ADF_META_PIPELINE_RUNS'
T_ADF_META_PIPELINE_RUNS_STG   = 'T_ADF_META_PIPELINE_RUNS_STG'
T_ADF_META_ACTIVITY_RUNS       = 'T_ADF_META_ACTIVITY_RUNS'
T_ADF_META_ACTIVITY_RUNS_STG   = 'T_ADF_META_ACTIVITY_RUNS_STG'
T_ADF_META_TRIGGER_RUNS        = 'T_ADF_META_TRIGGER_RUNS'
T_ADF_META_TRIGGER_RUNS_STG    = 'T_ADF_META_TRIGGER_RUNS_STG'
T_ADF_META_TRIGGER_MASTER      = 'T_ADF_META_TRIGGER_MASTER'
T_ADF_META_EVENT_TRIGGER       = 'T_ADF_META_EVENT_TRIGGER'
T_ADF_META_SCHEDULE_TRIGGER    = 'T_ADF_META_SCHEDULE_TRIGGER'
T_ADF_META_TUMBLINGWINDOW_TRIGGER ='T_ADF_META_TUMBLINGWINDOW_TRIGGER'
T_ADF_META_LINKED_SERVICES   = 'T_ADF_META_LINKED_SERVICES'
T_ADF_META_DATASETS  = 'T_ADF_META_DATASETS'


## Columns 
COLUMNS_T_ADF_META_PIPELINES     =       ['ID','NAME','TYPE','PROPERTIES','ETAG', 
                                          'ETL_INSERT_TS','ETL_UPDATE_TS','ETL_INSERT_ID','ETL_UPDATE_ID' ]



COLUMNS_T_ADF_META_TRIGGER_MASTER =  ['ADDITIONAL_PROPERTIES', 'ID', 'NAME', 'ETAG', 'TYPE', 'DESCRIPTION',
                                        'RUNTIME_STATE', 'ANNOTATIONS', 'PIPELINES_AND_PARAMS', 'ETL_INSERT_TS',
                                        'ETL_UPDATE_TS', 'ETL_INSERT_ID', 'ETL_UPDATE_ID']


COLUMNS_T_ADF_META_EVENT_TRIGGER =  ['ADDITIONAL_PROPERTIES', 'ID', 'NAME', 'BLOB_PATH_BEGINS_WITH', 'BLOB_PATH_ENDS_WITH', 'IGNORE_EMPTY_BLOBS',
                                        'EVENTS', 'SCOPE', 'ETL_INSERT_TS',
                                        'ETL_UPDATE_TS', 'ETL_INSERT_ID', 'ETL_UPDATE_ID']



COLUMNS_T_ADF_META_SCHEDULE_TRIGGER         =  ['ADDITIONAL_PROPERTIES', 'ID', 'NAME', 'FREQUENCY', 'INTERVAL', 'START_TIME',
                                                'END_TIME', 'TIME_ZONE', 'SCHEDULE_TIMES','SCHEDULE_WEEK_DAYS','SCHEDULE_MONTH_DAYS',
                                                'SCHEDULE_MONTHLY_OCCURRENCES','ETL_INSERT_TS',
                                                'ETL_UPDATE_TS', 'ETL_INSERT_ID', 'ETL_UPDATE_ID']



COLUMNS_T_ADF_META_TUMBLINGWINDOW_TRIGGER   =  ['ADDITIONAL_PROPERTIES', 'ID', 'NAME', 'FREQUENCY', 'INTERVAL', 'START_TIME',
                                                'END_TIME','TIME_ZONE', 'DELAY', 'MAX_CONCURRENCY','RETRY_POLICY_PROPERTIES','RETRY_POLICY_COUNT','RETRY_POLICY_INTERVAL_IN_SECONDS',
                                                'DEPENDS_ON','SCHEDULE_TIMES','ETL_INSERT_TS',
                                                'ETL_UPDATE_TS', 'ETL_INSERT_ID', 'ETL_UPDATE_ID']



COLUMNS_T_ADF_META_PIPELINE_RUNS =       ['ADDITIONAL_PROPERTIES','RUN_ID','RUN_GROUP_ID','IS_LATEST',
                                            'PIPELINE_NAME', 'PARAMETERS','RUN_DIMENSIONS','INVOKED_BY',
                                            'LAST_UPDATED','RUN_START','RUN_END',
                                            'DURATION_IN_MS','DURATION_HH_MI_SS','STATUS','MESSAGE',
                                            'ETL_INSERT_TS','ETL_UPDATE_TS','ETL_INSERT_ID','ETL_UPDATE_ID'
                                            ]


COLUMNS_T_ADF_META_ACTIVITY_RUNS =  ['ADDITIONAL_PROPERTIES','PIPELINE_NAME','PIPELINE_RUN_ID','ACTIVITY_NAME',
                                    'ACTIVITY_TYPE','ACTIVITY_RUN_ID','LINKED_SERVICE_NAME','STATUS',
                                    'ACTIVITY_RUN_START','ACTIVITY_RUN_END','DURATION_IN_MS','DURATION_HH_MI_SS','INPUT','OUTPUT','ERROR',
                                    'ETL_INSERT_TS','ETL_UPDATE_TS','ETL_INSERT_ID','ETL_UPDATE_ID']



COLUMNS_T_ADF_META_TRIGGER_RUNS =   ['ADDITIONAL_PROPERTIES','TRIGGER_RUN_ID','TRIGGER_NAME','TRIGGER_TYPE',
                    'TRIGGER_RUN_TIMESTAMP', 'STATUS','PROPERTIES','TRIGGERED_PIPELINES',
                    'RUN_DIMENSION','DEPENDENCY_STATUS',
                    'ETL_INSERT_TS','ETL_UPDATE_TS','ETL_INSERT_ID','ETL_UPDATE_ID'
                    ]

COLUMNS_T_ADF_META_LINKED_SERVICES  = ['ID','NAME','TYPE','PROPERTIES','ETAG', 
                                          'ETL_INSERT_TS','ETL_UPDATE_TS','ETL_INSERT_ID','ETL_UPDATE_ID' ]


COLUMNS_T_ADF_META_DATASETS  = ['ID','NAME','TYPE','PROPERTIES','ETAG', 
                                          'ETL_INSERT_TS','ETL_UPDATE_TS','ETL_INSERT_ID','ETL_UPDATE_ID' ]

## Date and User ID parameters
TZ          = pytz.timezone('UTC')
ETL_TIME    = datetime.datetime.now(TZ).isoformat()
ETL_ID      = os.environ["AME_SNW_USERNAME"]


