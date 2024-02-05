# Python core libs
import os 
import traceback
import time
import sqlite3
from sqlalchemy import inspect, text

# Installed libs
from dotenv import load_dotenv
#import logging
from loguru import logger
from fastapi import FastAPI, BackgroundTasks, Request
import ray
import sys 

# Insights libs
from insights.util.config import load_validate_config
from insights.model.config import *
from insights.model.jobs import Job
from insights.db import Database
import insights.framework.sqltodf as sqltodf
import json
from datetime import datetime 
import hashlib
import insights.util.base as base
# Initialize service layer
load_dotenv()
app = FastAPI()
#logger = logging.getLogger(__name__)




# # TODO - RAY or Multithreading will be used, setthis up accordingly
# if ray.is_initialized(): 
#     pass #ray.init(address='auto')
# else:
#     ray.init()

# DB is determined by the environment variable DB_PATH
#db_path = os.getenv("INSIGHTS_JOB_DATABASE")

#logger.info(f"db_path = {db_path}")

# connection = None
# if db_path:
#     conn_str = db_path
#     #connection = sqlite3.connect("athena.db")
# else:       
#     conn_str = "file::memory:?cache=shared"
#     # connection = sqlite3.connect("file::memory:?cache=shared", check_same_thread=False)
#     logger.info(f"using in memory db")

def create_job(conf: Configuration, config_name):
    
    j = Job()
    j.Config = conf.json()
    j.StartTime = datetime.now()
    j.ConfigName = config_name
    hash = hashlib.sha256(j.Config.encode())
    j.ConfigHash = hash.hexdigest()
    j.id = base.save_object(conf, j, Job)
    return j

@ray.remote
def run_stratify(ctx, job_id):

    try:
      start = time.time()
    #   config = pyhocon.ConfigFactory.from_dict(conf)
      
    #   db_handler = DBHandler(config, logging.WARN, job.id)
    #   logger.addHandler(db_handler)
      sqltodf.run(ctx)
      elapsed = time.time() - start 

      logger.info(f"Completed creating histogram in {elapsed} secs")
    except Exception as e:
      logger.error(f"Unhandled error in sqltodf.run{traceback.format_exc()}") 
      base.update_job_error(job_id, str(e))


@app.post("/config_validate")
async def config_validate(request: Request):
    
    # Load the body string from the request. This will be saved with the job as a raw string.
    # Load and validation will check that the body which contains configuration is proper json and a valid Configuration object
    config_raw = await request.body() # -> str
    
    # Validate and load the config into pydantic model 
    config = load_validate_config(config_raw)   # -> Configuration

    if config is None:
        return {"valid": "False"}
    else:
        return {"valid": True}
    

@app.post("/run_job/stratify")
async def stratify(request: Request):
    
    # Load the body string from the request. This will be saved with the job as a raw string.
    # Load and validation will check that the body which contains configuration is proper json and a valid Configuration object
    # Load the job function from the config run_type parameter

    try:
        config_raw = await request.body() # -> str

        # Validate and load the config into pydantic model
        config = load_validate_config(config_raw)   # -> Configuration

        if config is None:
            print(f"Invalid config")
            return {"job_id": -1}
         
        # Save the request and get the job id
        config = Configuration.parse_obj(config)
        job = create_job(config, config.name)

        ctx = Context(config=config, job_id=job.id)

        #job_func = ray.remote(sqltodf.run)
        ref = run_stratify.remote(ctx, job.id)
    except Exception as e:
        print(f"error running job \m {traceback.format_exc()}")
        return {"job_id": -1}
    
    return {"job_id": job.id}

# @app.get("/stratify/results")
# async def get_results(request: Request):

#     config_raw = await request.body() # -> str

#     # Validate and load the config into pydantic model
#     config = load_validate_config(config_raw)   # -> Configuration
    
#     engine = base.get_alchemy_engine(config, config.src_server)
    
#     inspection = inspect(engine)
#     table_names = inspection.get_table_names()

#     data = {}
#     with engine.connect() as conn:
#         for t in table_names:
#             query = "SELECT * from " + t
#             cursor = conn.execute(text(query))
#             cols = tuple(cursor.keys())
#             rows = cursor.fetchall()
#             #rows.insert(0, cols)
#             zipped_data = list([dict(zip(cols, row)) for row in rows])
#             data[t] = zipped_data
        
#     # print(f"Timestamp {data['timestamp']}")
#     return data
