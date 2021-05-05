'''

#   ^           _
#  /_\  |\  /| |_
# /   \ | \/ | |_
#          


    Description  : API Entry point for ADF MetaExtractor
    Last updated    : 3/11/2021
    Deployment      : Terraform
    Updated by   : Thomas Mathew.
    This is the entry point for ADFMetaExtractor. The HTTP Body is evaluated and routed based on api_name parameter
   
'''

import logging
import json
import azure.functions as func
from .common_variables import *
from .get_pipelines import get_pipelines,get_exception_message
from .get_datasets import get_datasets


def main(req: func.HttpRequest) -> func.HttpResponse:
    try:
        func_response = {}
        status_code = 400
        logging.info('****** ADF Meta Extractor API ******')
        req_body = req.get_body().decode()
        payload = json.loads(req_body)
        requested_api_name = payload.get('api_name')
        if requested_api_name not in AVAILABLE_API_LIST:
            invalid_req_mesg = f"Invalid API name. Available APIs : {str(AVAILABLE_API_LIST)}"
            return func.HttpResponse(invalid_req_mesg,mimetype='application/json',status_code=400)

        if requested_api_name == 'GetDatasets' : 
            logging.info(f"Invoking {requested_api_name} API")
            func_response = get_pipelines(payload)
        elif requested_api_name == 'GetTriggers' :
            logging.info(f"Invoking {requested_api_name} API")
            func_response = get_triggers(payload)
        output_json = json.dumps(func_response)
        status_code = func_response['status_code']
        return func.HttpResponse(
                output_json,
                status_code=status_code
            )
    except Exception as e:
        return func.HttpResponse(
                str(e),
                status_code=400
            )
