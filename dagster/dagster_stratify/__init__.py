from dagster import job, schedule, RunRequest, Definitions
from . import ops

@job
def config_validation_job():
    ops.config_validation_op()

@job
def computations_job():
    ops.computations_op()

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
