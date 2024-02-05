import abc
from pydantic import BaseModel, Extra
from typing import Any, List, Optional, TypeVar, Union
from collections import defaultdict

class MyBaseModel(BaseModel):

    class Config:
        allow_population_by_field_name = True
        extra = Extra.allow

class StratTypeMappingItem(BaseModel):
    type: str
    name: str
    value: str
    bin_count: Optional[int] = None
    top_count: Optional[int] = None
    max_unique: Optional[int] = None

class Server(MyBaseModel): 
    name: str
    database: str
    dialect: str
    driver: Optional[str] = None
    host: Optional[str] = ""
    password: Optional[str] = None
    user: Optional[str] = None
    port: Optional[str] = None
    schemas: Optional[list[str]] = []

class DataMap(MyBaseModel):
    pass
    
class Computation(BaseModel, abc.ABC):
    name: str
    value: str
    class_name: Optional[str] = ""
    columns: Optional[List[str]] = []
    top_count: Optional[int] = None
    max_unique: Optional[int] = None
    binning_algo: Optional[str] = ""
    bin_count: Optional[int] = None
    bins: Optional[List[float]] = []

class TopDistinct(Computation):
    top_count: Optional[int] = None
    max_unique: Optional[int] = None
    
class Histogram(Computation):
    binning_algo: Optional[str] = ""
    bin_count: Optional[int] = None
    bins: Optional[List[float]] = []
     
class Configuration(MyBaseModel):
    run_type: Optional[str] = ""
    servers: Optional[List[Server]] = []
    datatype_mapping: Optional[List[DataMap]]
    default_partition_key: Optional[str] = None
    computations: Optional[List[Computation]] = []
    

class StratifyConfig(Configuration):
     strat_type_mapping: List[StratTypeMappingItem]
     servers: list[Server]
     src_server: str 

class Context(MyBaseModel):
    config: Configuration
    job_id: int
    runtime_params: Optional[dict] = {}
    results: Optional[defaultdict] = defaultdict(dict)

    def __getattr__(self, name):
        #print("checking attributes")
        if hasattr(self.config, name):
            value = getattr(self.config, name)
            return value
        else:
            try:
                context_val = object.__getattribute__(self, name)
                return context_val
            except AttributeError:
                return None
        
        
def get_from_list(lyst: str, name: str) -> BaseModel: 
    for item in lyst:
        if item.name == name:
            return item
    return None