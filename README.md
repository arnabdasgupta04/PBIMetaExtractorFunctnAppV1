Azure Data Factory Metadata Extractor
-

ADF Meta Extractor extracts ADF metadata through REST APIs using the Python library azure-mgmt-datafactory. Below are the endpoints for data extraction.

Metadata            |    API Name
-
Pipeline Runs       |   GetPipelineRuns
Activity Runs       |   GetActivityRuns
Pipelines           |   GetPipelines
Triggers            |   GetTriggers
TriggerRuns         |   GetTriggerRuns
Linked Services     |   GetLinkedServices
Datasets            |   GetDatasets


API Contract : 
-

Endpoint : https://<azurefuncappname>.azurewebsites.net/
Method   : POST
Content-Type: application/json

    {
                "api_name"         : <API Name>,
                "resource_group"   : <Resource Group Name>,
                "factory_name"     : <Data factory name>,
                "api_limit"        : <API page limit on Azure Data Factory API Pagination will be capped at this value>
                "watermark_offset" : <Number of days to go back from max(date) in table>. Eg : if watermark_offset=2, "previous_date" = max(etl_insert_ts from table) - 2 and   "current_date" = datetime.now(). Data extraction DataFactory API will then use previous_date and current_date to filter response.

    }


How to call the endpoints ?
-

POST http://localhost:7071/api/ADFMetaExtractorCore
Content-Type: application/json

    {
                "api_name"         : "GetPipelines",
                "resource_group"   : "cdp-sedw.rg",
                "factory_name"     : "lllcdpsedwadf1",
                "api_limit"        : 100,
                "watermark_offset" : 1

    }

Folder structure :
-
 - ADFMetaExtractorCore
   - __init__.py            |    API Entry point. Resolve JSON body of POST method and invoke methods based on requested API name
   - common_functions.py    |    Contains the reusable python functions for data extraction.
   - common_variables.py    |    Contains table names and environment variables obtained by Key Vault integration
   - get_pipelines.py       |    Get the pipeline name and properties within a datafactory.
   - get_activity_runs.py   |    Get the activity runs based on a pipeline id and time frame.
   - get_pipeline_runs.py   |    Get the pipeline runs based on data factory and time frame.
   - get_triggers.py        |    Get the scheduled trigger, tumbling window, event triggers for a given data factory.
   - get_trigger_runs.py    |    Get the trigger runs based on data factory and time frame.
   - get_linked_services.py |    Get linked services for a given data factory
   - get_datasets.py        |    Get datasets for a given data factory
   - requirements.txt       |    Includes all dependent libraries.
   - Tests                  |    Contains Sample HTTP requests to test the endpoints.
   - local_settings.json    |    Contains environment variables. This wont be deployed. Function->Configuration->Application Settings hold the same value.


Azure Functions Info :
-

Python Runtime version          |   3.8
Plan                            |   Consumption
Function Timeout                |   10 min
ADF AzFunc Timeout              |   230 seconds (Cannot be changed. Workaround is using Durable Function and Web activity combo)
Extension Bundle (host.json )   |   "version": "[2.*, 3.0.0)"


Table Names :
-

"META_DB"."DNA"."T_ADF_META_ACTIVITY_RUNS"
"META_DB"."DNA"."T_ADF_META_PIPELINES"
"META_DB"."DNA"."T_ADF_META_PIPELINE_RUNS"
"META_DB"."DNA"."T_ADF_META_EVENT_TRIGGER"
"META_DB"."DNA"."T_ADF_META_SCHEDULE_TRIGGER"
"META_DB"."DNA"."T_ADF_META_TRIGGER_MASTER"
"META_DB"."DNA"."T_ADF_META_TUMBLINGWINDOW_TRIGGER"
"META_DB"."DNA"."T_ADF_META_TRIGGER_RUNS"
"META_DB"."DNA"."T_ADF_META_LINKED_SERVICES"
"META_DB"."DNA"."T_ADF_META_DATASETS"
