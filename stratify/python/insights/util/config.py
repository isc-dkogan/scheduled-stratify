import pyhocon
from insights.model.config import *
import pydantic
import traceback
from loguru import logger
import json

def load_validate_config(config_raw: str):

    try:
        config = json.loads(config_raw)
    except Exception as e:
        logger.critical(f"request body format is not valid json \n {traceback.format_exc()}")
        return None
    
     # load the config into a pyhocon object for variable interpolation including env vars
    config_hocon = pyhocon.ConfigFactory.from_dict(config)

    try:
        config = Configuration.parse_obj(config_hocon.as_plain_ordered_dict())

    except pydantic.ValidationError as e:
        logger.critical(f"error parsing config \n {traceback.format_exc()}")
        #print(f"error parsing config \n {traceback.format_exc()}")
        return None

    return config
