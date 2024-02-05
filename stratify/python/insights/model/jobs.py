from sqlalchemy import Column, Float, Integer, String, TIMESTAMP, Table
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()
metadata = Base.metadata

class Job(Base):
    __tablename__ = 'Job' 

    id = Column(Integer, primary_key=True)
    #JobID = Column(Integer)
    ConfigName = Column(String(200))
    StartTime = Column(TIMESTAMP)
    Config = Column(String(10000))
    EndTime = Column(TIMESTAMP)
    ConfigHash = Column(String(256))
    Runtime = Column(String(200))
    HasErrors = Column(Integer)
    ErrorMessage = Column(String(10000))
    

class Histogram(Base):
    __tablename__ = 'Histogram'
    
    id = Column(Integer, primary_key=True)
    db_instance = Column(String(50))
    db_name = Column(String(50))
    table_name = Column(String(50))
    field_name = Column(String(50))
    job_id = Column(Integer)
    bucket_number = Column(Integer)
    minval = Column(Float)
    maxval = Column(Float)
    count = Column(Integer)

class TableStats(Base):
    
    __tablename__ = 'Table_Stats'
    
    id = Column(Integer, primary_key=True)
    job_id = Column(Integer)
    database_name = Column(String(50))
    schema_name = Column(String(50))
    table_name = Column(String(50))
    field_count =  Column(Integer)
    full_name = Column(String(50))
    row_count =  Column(Integer)
    included = Column(Integer)


class TableSchema(Base):

    __tablename__ = 'Table_Schema'
    
    id = Column(Integer, primary_key=True)
    job_id =  Column(Integer)
    db_name = Column(String(50))
    schema_name = Column(String(50))
    table_name = Column(String(50))
    column_name = Column(String(50))
    data_type = Column(String(50))
    character_maximum_length = Column(Integer)
    UNIQUE_COLUMN = Column(String(50))
    is_nullable = Column(String(50))
    primary_key = Column(String(50))
    is_identity = Column(String(50))
    index_name = Column(String(50))
    ordinal_position = Column(Integer)
    non_unique = Column(String(50))
    min = Column(Float)
    max = Column(Float)
    avg = Column(Float)
    stddev = Column(Float)
    distinctcount = Column(Integer)
    strat_names = Column(String(255))

class TopCounts(Base):
    
    __tablename__ = 'TopCounts' 
    
    id = Column(Integer, primary_key=True)
    db_instance = Column(String(50))
    db_name = Column(String(50))
    table_name = Column(String(50)) 
    field_name = Column(String(50))
    job_id = Column(Integer)
    rank = Column(Integer)
    value = Column(String(300))
    count = Column(Integer)