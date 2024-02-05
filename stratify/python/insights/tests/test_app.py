from fastapi.testclient import TestClient
import pytest
from insights.rest.main import app
from insights.model import config
import insights.rest.main as rest_main
import json

client = TestClient(app)

def get_stratify_config():
    stratdict =  {
          "type": "integer",
          "name": "histogram",
          "value": "$1",
          "bin_count": 10
        } 
    
    servers = [
      {"name": "b360",
      "database": "b360",
      "driver": "com.intersystems.jdbc.IRISDriver",
      "host": "localhost",
      "password": "test",
      "user": "superuser",
      "port": 1972,
      "schemas": []
        }
    ] 

    strat = config.StratTypeMappingItem(**stratdict)
    conf = config.StratifyConfig(run_type="stratification", strat_type_mapping=[strat], servers=servers, src_server="b360")
    return conf

def test_stratify(mocker):
    
    config = get_stratify_config()
    mocker.patch('insights.rest.main.get_job', return_value=25)

    response = client.post("/run_job/stratify", data=config.json())

    print(f"Response: {response.json()}")
    assert response.status_code == 200

    
