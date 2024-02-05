from __future__ import annotations
from typing import List
from pydantic import BaseModel
from datetime import datetime
import time
from pydantic.schema import Optional
from insights.model.config import MyBaseModel
class Parent(MyBaseModel):
    list_name: str

    def get_child(self, name: str):
        lyst = getattr(self, self.list_name)
        for item in lyst:
            if item.name == name:
                return item
        return None
    
    def add(self, child: BaseModel):
        lyst = getattr(self, self.list_name)
        lyst.append(child)
        
    def get_single(self):
        # Often there will be just one
        lyst = getattr(self, self.list_name)
        return lyst[0]
                
class Column(MyBaseModel):

    name: str
    instance_name: str 
    timestamp: Optional[datetime] = None
    database_name: str  
    schema_name: str  
    table_name: str 
    data_type: str  
    character_maximum_length: Optional[int]
    unique_column: Optional[bool]
    is_nullable: Optional[bool]
    primary_key: Optional[bool]  
    is_identity: Optional[bool]  
    index_name: Optional[str] 
    ordinal_position: Optional[int]  
    non_unique: Optional[bool]  
    
    def add_data(self, values: dict):
        for k,v in values.items():
            attr = hasattr(self, k)
            if attr:
                setattr(self, k, v)


class Table(Parent):
    name: str
    field_count: int = 0
    full_name : str = ""
    row_count : int = 0
    schema_name : str = ""
    columns: List[Column] = []
    list_name = "columns"
    computations: List[str] = []
   # partition_key: Optional[str] = None

class Schema(Parent):
    name: str
    tables: List[Table] = []
    list_name = "tables"
    

class Database(Parent):
    name: str
    schemas: List[Schema] = []
    list_name = "schemas"
    
    def get_tables(self):
        tables = []
        for s in self.schemas:
            for t in s.tables:
                tables.append(t)
        return tables

class Instance(Parent):
    name: str
    host: str = ""
    databases: List[Database] = []
    list_name = "databases"

    
class Metadata(Parent):
    """_summary_
    Metadata->Instances->Databases->Schemas->Tables->Columns
    """
    timestamp: timestamp = time.time()
    instances: List[Instance] = []
    list_name = "instances"
    
    def get_db(self):
        return self.get_single().get_single()
    
    def get_tables(self):
        db = self.get_single().get_single()
        return db.get_tables()
