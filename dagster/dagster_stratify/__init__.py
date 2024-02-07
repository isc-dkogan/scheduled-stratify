from dagster import op, job, schedule, get_dagster_logger, RunRequest, Definitions
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

    config_str = get_config_str_from_file_op("./config.conf")
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

    config_str = get_config_str_from_file_op("./config.conf")
    headers =  {"accept":"application/json"}
    response = requests.post(api_url, data=config_str, headers=headers)

    my_logger.info(f"Job {response.json()['job_id']} is running")


@job
def config_validation_job():
    config_validation_op()

@job
def computations_job():
    computations_op()

@schedule(job=config_validation_job, cron_schedule="15 18 * * 1-5")
def config_validation_schedule():
    return RunRequest()

@schedule(job=computations_job, cron_schedule="6 15 * * 1-5")
def computations_schedule():
    return RunRequest()
    
defs = Definitions(
    jobs=[config_validation_job, computations_job],
    schedules=[config_validation_schedule, computations_schedule],
)
