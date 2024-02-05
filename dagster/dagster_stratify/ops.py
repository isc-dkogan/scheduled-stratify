from dagster import op, get_dagster_logger
import json
import requests

@op
def get_config_str_from_file_op(filepath: str):
    f = open(filepath)

    config = json.load(f)
    return json.dumps(config)

@op
def config_validation_op():
    my_logger = get_dagster_logger()

    api_url = "http://localhost:8000/config_validate"

    config_str = get_config_str_from_file_op("/Users/dkogan/stratify_config.conf")
    headers =  {"accept":"application/json"}
    response = requests.post(api_url, data=config_str, headers=headers)

    if response.json()['valid']:
        my_logger.info("Configuration is valid")
    else:
        my_logger.error("Configuration is not valid")

@op
def computations_op():
    my_logger = get_dagster_logger()

    api_url = "http://localhost:8000/run_job/stratify"

    config_str = get_config_str_from_file_op("/Users/dkogan/stratify_config.conf")
    headers =  {"accept":"application/json"}
    response = requests.post(api_url, data=config_str, headers=headers)

    my_logger.info(f"Job {response.json()['job_id']} is running")
