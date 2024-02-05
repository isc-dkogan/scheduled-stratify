import jaydebeapi
from insights.model.metadata import Metadata, Database, Schema, Table, Column, Metadata, Instance
import math 
import ray
from insights.model.config import Configuration, get_from_list, Server, Context, Histogram, TopDistinct
import insights.model.jobs as jobs
import logging
from sqlalchemy import create_engine,  MetaData
from sqlalchemy.orm import Session
import types
from collections.abc import Iterable
from functools import partial
import sqlite3
from insights.framework.compute import *
import pandas as pd
import time

from loguru import logger
# logger.remove()
# logger.add(sys.stderr, level="DEBUG")

# logger = logging.getLogger(__name__)
# logger.setLevel(logging.DEBUG)
    
############################################################################################################################
# METADATA FUNCTIONS
############################################################################################################################
def get_metadata(ctx: Context):
    """_summary_
    Uses the src_server from the config to deteminre datasource and loads metadata for all schemas specified.
    Args:
        ctx (Context): Context object

    Returns:
        insights.model.Metadata: Contains a heirarchy of instance->database->schema->table->column
    """
    server = get_from_list(ctx.config.servers, ctx.config.src_server)
    
    alchemy_metadata = get_metadata_alchemy(ctx, server)
    metadata = load_metadata_to_models(alchemy_metadata, server)
    
    return metadata

def get_metadata_alchemy(ctx: Context, server: Server = None):
     
     if not server:
        server = get_from_list(ctx.config.servers, ctx.config.src_server)
        
     connection_url = create_connection_url(server)
     engine = create_engine(connection_url)
     
     metadata_obj = MetaData()
     if server.schemas:
        for schema in server.schemas:
            metadata_obj.reflect(engine, schema)
     else:
        metadata_obj.reflect(engine)
        
     return metadata_obj

def load_metadata_to_models(alchemy_metadata: MetaData, server: Server):
    
    metadata = Metadata()
    instance = Instance(name=server.name, host=server.host)
    metadata.instances.append(instance)
    
    database = Database(name=server.database)
    instance.databases.append(database)
    
    if server.schemas:
        for schema_name in server.schemas:
            schema = Schema(name=schema_name)
            database.schemas.append(schema)
    else:
        schema = Schema(name="default")
        database.schemas.append(schema)
    
    for src_table_name, src_table in alchemy_metadata.tables.items():
        # Add the table to the schema
        logger.error(f"loading table {src_table.fullname}")
        table_schema = src_table.schema if src_table.schema else "default"
        table = Table(name=src_table_name, full_name=src_table.fullname)
        schema = get_from_list(database.schemas, table_schema)
        if schema:
            schema.tables.append(table)
            
            table_field_count = 0
            # Add the columns to the table
            for col_name, col in src_table.columns.items():
                logger.debug(f"{col_name}, {col.type}, {type(col.type)}, {str(col.type)}, {col.index}, {col.unique}, {col.primary_key}, {col.nullable}")
                
                column = Column(name=col_name, instance_name=instance.name, database_name=database.name, 
                                schema_name=table_schema, table_name=src_table_name, data_type=str(col.type),
                                is_nullable=col.nullable, primary_key=col.primary_key, unique_column=col.unique)
                
                table.columns.append(column)
                table_field_count += 1
                
            setattr(table, "field_count", table_field_count)
        else:
            logging.debug("Skipping as schema does not exist")
        
    return metadata

###########################################################################################################################
# CONNECTION FUNCTIONs
###########################################################################################################################

def get_alchemy_engine(config: Configuration, server_name):  
    
    server = get_from_list(config.servers, server_name)
    
    connection_url = create_connection_url(server)
    engine = create_engine(connection_url)
 
    return engine

def create_connection_url(server: Server):
     # Create a connection url from the server properties in this form dialect+driver://username:password@host:port/database
     # Only adding sections if they have a value in the server instance
     
     # sqlite requires 3 slashes for reference to file db
     seperator = ":///" if server.dialect == "sqlite" else "://"
     driver_dialect = f"{server.dialect}{seperator}" #if not server.driver else f"{server.dialect}+{server.driver}{seperator}"
     user_pass = f"{server.user}:{server.password}@" if server.user and server.password else ""
     host_port = f"{server.host}:{server.port}/" if server.host and server.port else ""
     database = f"{server.database}"

     url = driver_dialect+user_pass+host_port+database

     logger.debug(f"Created connection URL: {url}")
     
     return url

def get_connection(config, conn_type, instance = ""):
    logger.debug(f"Getting {conn_type} connection")

    server_name = instance if instance else config.src_server
    if conn_type == "jdbc":
        return get_jdbc_connection(config, server_name)
    elif conn_type == "sqlite":
        return get_sqlite_connection(config, server_name)
    else:
        logger.warning(f"Connection type {conn_type} not supported")
        raise Exception(f"Connection type {conn_type} not supported")

def get_jdbc_connection(config: Configuration, server_name: str):
    server = get_from_list(config.servers, server_name)
    db = f"{server.host}:{server.port}/{server.database}"
    conn_url = f"jdbc:IRIS://{db}"

    logger.debug(f"Connection URL {conn_url}")
    conn = jaydebeapi.connect(server.driver,
                            conn_url,
                            [server.user, server.password],
                            config.spark_jars)

    return conn

def get_sqlite_connection(config, server_name):
     server = get_from_list(config.servers, server_name)

     logger.debug(f"Connecting to DB {server.database}")
     conn = sqlite3.connect(server.database)

     return conn
 
###########################################################################################################################
# METADATA ENRICHMENT FUNCTIONs
###########################################################################################################################

def clean_data_type(data_type: str):
    # Remove all characters after first (, VARCHAR(50) = VARCHAR, DECIMAL(10,2) = DECIMAL
    lastchar = data_type.find('(')
    if lastchar != -1:
        data_type = data_type[:lastchar]
    
    return data_type

def partition_key_only_aggregate_requirements(ctx: Context):

    # Set only partition_key_agg_funcs - probably only min, max - on the partition_key column
    for table in ctx.metadata.get_tables():
        partition_key = get_partition_key(ctx, table)
        column = get_from_list(table.columns, partition_key)
        setattr(column, 'agg_funcs', getattr(ctx, "partition_key_agg_funcs"))
                   
def datatype_aggregate_requirements(ctx):
    """"
    This function determines what aggregates are required based on the these objects in the 
    config - datatype_mapping, which have a mapping from column types to a base type
    and a list of agg_funcs. If a column's datatype is in a super_datatypes.subtypes then
    all of the agg_funs are added to the list of aggregates to compute
    """
    # Iterate through all columns in the metadata and check if the datatype maps to any 
    # subtypes in the any of the datatype_mapping objects. If they match, then add all of the agg_funcs
    # to the list of aggregates to compute as additional metdata on the column
    for table in ctx.metadata.get_tables():
        logger.debug(f"Table {table.name}")
        for column in table.columns:
            logger.debug(f"Column {column.name}")
            logger.debug(f"Datatype mapping: {ctx.datatype_mapping}")
            for datatype_map in ctx.datatype_mapping:
                if clean_data_type(column.data_type) in datatype_map.subtypes:
                    # If more than one can match, this could already exist so they get appended instead of created
                    if hasattr(column, 'agg_funcs'):
                        column.agg_funcs.extend(datatype_map.agg_funcs)
                    else:
                        setattr(column, 'agg_funcs', datatype_map.agg_funcs)
                    logger.debug(f"Aggregate Functions: {column.agg_funcs}")
            
def create_aggregate_sql(ctx: Context, set_required_aggregates: types.FunctionType):
    """The column aggregate data gets stored in the metadata table with each column
    What aggregations need to be loaded are dependant on the calculations, usually those
    being executed on that column. Because of this dependency between the calculations,
    which all of their own requirements on aggregate data, a function is required to handle
    insuring these dependencies are met.

    Args:
        ctx (Context)
        aggregation_function (types.FunctionType): A function that determines the SQL aggregations to be applied to each column

    Returns:
        _type_: Context object
    """
    set_required_aggregates(ctx)
    #list_agg_funcs_on_column(ctx)
    
    for table in ctx.metadata.get_tables():
        sql = f"SELECT "
        partition_key = get_partition_key(ctx, table)
        for column in table.columns:
            if hasattr(column, "agg_funcs"):
                for agg in column.agg_funcs:
                    func = f"{agg} ".replace("$1", column.name)
                    name = get_name_from_agg_func(agg)
                    clause = f"{func} {name}_{column.name},"
                    sql += clause

        sql = sql[:-1]
        sql += f" FROM {table.full_name}"
        clause = ""
        if ctx.sample_rate > 0:
            clause = f" WHERE mod({partition_key}, {ctx.sample_rate}) = 0 "
        sql += clause 
        setattr(table, "agg_sql", sql)
    
    return ctx
    # for table in ctx.metadata.get_tables():
    #     agg_sql = table.agg_sql if hasattr(table, "agg_sql") else ""
    #     logger.debug(f"{table.full_name}: {agg_sql}")

###########################################################################################################################
# Computations
###########################################################################################################################
def create_computations(ctx: Context):
    """USes the set_computations function to create a computations object in the context

    Args:
        ctx (Context): 
        set_computations (types.FunctionType, optional): A function which handles how computations are created. Defaults to datatype_map_computations.

    Returns:
        _type_: Context
    """
    
    for table in ctx.metadata.get_tables():
        # Create the copmutation list for the table if it does not exist
        if not hasattr(table, "computations"):
            setattr(table, "computations", [])
        for column in table.columns:
            # For every column in the table, check if its datatype matches in the computation_type_mapping object
            for compute_map in ctx.computation_type_mapping:
                datatype = clean_data_type(column.data_type) 
                if datatype == compute_map.get("name"):
                    # If the datatype matches, we get all computations in the compute_map.computations list
                    # This is likely to have only one entry
                    for compute_name in compute_map.get("computations"):
                        # We need to add this column to the table.computation[compute_name].columns list
                        # If the compute type does not exists we make a copy from the context and copy onto table
                        table_compute_obj = get_from_list(table.computations, compute_name)
                        if not table_compute_obj:
                            ctx_compute_obj = get_from_list(ctx.computations, compute_name)
                            table_compute_obj = ctx_compute_obj.copy() 
                            table.computations.append(table_compute_obj)
                        
                        # Then we add the column to the new or existing list
                        compute_columns_list = getattr(table_compute_obj, "columns")
                        if not compute_columns_list:
                            setattr(table_compute_obj, "columns", [])
                        
                        table_compute_obj.columns.append(column.name)
                        column_compute_obj = table_compute_obj.copy().dict()
                        del column_compute_obj["columns"]
                        # Add the compute object to the column with the name only. This is a placeholder for any column specific data
                        # required for the computation. For example, a histogram can require pre-computed bins for each column
                        column_compute_list = get_or_create(column, "computations", [])
                        class_ = lookup_function(table_compute_obj.class_name)
                        column_compute_obj = class_(**column_compute_obj)
                        column_compute_list.append(column_compute_obj)
                        
                        
                        
        logger.debug(f"{table.full_name} computations: {table.computations}")
    return ctx

def get_or_create(base_obj, name:str, value: object):
    
    if not hasattr(base_obj, name):
        setattr(base_obj, name, value)

    obj = getattr(base_obj, name)
    return obj
    
def update_computations(ctx: Context, table: Table):
    """ Adds any additional metadata needed for the computations that relies on the aggregate data. For example,
    historgram computation can require the min and max for each column. This uses a naming convention of <compute_name>_prestep

    Args:
        ctx (_type_): Context
        table (_type_): Table

    Updates:
        ctx (_type_): Context
    """
    for compute in table.computations:
        prestep = lookup_function(f"{compute.name}_prestep")
        if not prestep:
            logger.debug(f"No prestep function for {compute.name}")
            return None
        
        # Execute the prestep function
        prestep(ctx, table, compute)

def naive_bins(minval, maxval, num_of_bins: int):
    rangev = maxval - minval
    size = rangev / num_of_bins
    bins = [minval + (size * i) for i in range(num_of_bins+1)]
    return bins

def clean_bins(minval, maxval, num_of_bins: int):
    """
    Creates num_of_bins and two more bins for high/low tail values
    This allows for evenly spaced bins and even bin borders
    """
    # Do this to make computation simpler
    # Later we will add an offset to correct
    if minval < 0:
        new_min = 0
        new_max = maxval + abs(minval)
    else:
        new_min = minval
        new_max = maxval
    
    rangev = new_max - new_min

    if rangev <= 1.2:
        bin_size = .1
        new_min = round(new_min,1)
    elif rangev <= 12:
        bin_size = 1
        new_min = round(new_min,-1)
    elif rangev <= 120:
        bin_size = 10
        new_min = round(new_min,-2)
    else:
        range_mag = math.floor(math.log10(abs(rangev)))
        bin_size = int(str(rangev)[:2]) * math.pow(10,range_mag-2)
        
    offset = 0 if minval > 0 or abs(minval) < bin_size else bin_size
    #print(minval, maxval, rangev, bin_size, offset)
    bins = [round(new_min + (bin_size * i) - offset,2) for i in range(num_of_bins+1)]
    
    # Determine if an additional bin is needed either end
    starting_value = bins[0]
    ending_value = bins[-1]
    #print(f"Starting and ending {starting_value}, {ending_value} and {minval} {maxval}")
    
    if minval < starting_value:
        bins.insert(0,minval)
        
    if maxval > ending_value:
        bins.append(maxval)
    
    return bins

def clean_bins_many(minval, maxval):
    """
    Creates between 10-50 bins with bin sizes of 1x10^X or 2x20^X
    """
    bins = []
    rangev = maxval - minval
    if rangev == 0:
        return bins
    
    range_mag = math.floor(math.log10(abs(rangev)))

    # if abs(minval) < 1 and abs(minval) > .01:
    #     minval1 = (round(minval,2) * 10) * .1 
    #     print(f"Changing minval from {minval} to {minval1}")
    # if rangev <= 100:
    #     size_pow = range_mag - 2
    # else:
    size_pow = range_mag - 1
        
    size = math.pow(10,size_pow)    
    binsize = size if rangev/size <= 50 else (size+size)
    
    if abs(minval) < 100 and abs(minval) > 0 and rangev >= 10:
        sign = 1 if minval >= 0 else -1
        first_digit = int(str(abs(minval))[0])
        if sign == 1:
            if minval == 1:
                new_min = 0
            else:  
                new_min = first_digit * 10
        else:
            new_min = (-first_digit - 1) * 10
    else:
        new_min = minval / size if minval != 0 else 0
        new_min = math.floor(new_min) * size

    new_max = maxval / size if maxval != 0 else 0
    new_max = math.ceil(new_max) * size

    new_range = round((new_max - new_min),3)
    bin_number = math.ceil(new_range / binsize)
    
    bins = [round(new_min + i*binsize,3) for i in range(bin_number+1)]

    if len(bins) > 20:
        bins = [i for idx,i in enumerate(bins,1) if idx % 2 != 0]
        if bins[-1] < new_max: bins.append(bins[-1]+(binsize*2))
    return bins
    
def get_bins(minval, maxval, num_of_bins, bin_algo):

    if minval is None or maxval is None:
        return []

    minval = float(minval)
    maxval = float(maxval)

    if bin_algo == 'naive':
        bins = naive_bins(minval, maxval, num_of_bins)
    elif bin_algo == 'clean':
        bins = clean_bins(minval, maxval, num_of_bins)
    elif bin_algo == 'clean_many':
        bins = clean_bins_many(minval, maxval)
    return bins

###########################################################################################################################
# SQL HELPER
###########################################################################################################################
def execute_sql_scalar(ctx: Context, sql: str, value_only=False):
    """Apply a SQL query to the database and return the single row result"""
    logger.debug(f"Executing query: {sql}")

    start = time.time()

    engine = get_alchemy_engine(ctx.config, ctx.config.src_server)
    conn = engine.connect()
    cursor = conn.exec_driver_sql(sql)
    
    result = cursor.fetchone()
    cols = cursor.keys()
    conn.close()

    elapsed = time.time() - start

    logger.info(f"Executed query in {elapsed} secs: {sql}")

    if value_only:
        return result
    else:
        return dict(zip(cols,result))

def get_name_from_agg_func(agg_func):
    nname = agg_func.replace("$1","").replace(")","").replace("(","").strip("_")
    return nname

def get_col_and_aggfunc(key: str):
    # Extract the aggfunc and column name from key from the form aggfunc_column
    vals = key.split("_", 1)
    agg_func = vals[0]
    colname = vals[1]
    return agg_func, colname
    
def get_partition_key(ctx: Context, table: Table):
    if hasattr(table, "partition_key"):
        return table.partition_key
    else:
        return ctx.config.default_partition_key

def extract_clause_from_agg_func(column, agg_func):
    func = f"{agg_func} ".replace("$1", column.name)
    name = get_name_from_agg_func(agg_func)
    clause = f"{func} {name}_{column.name},"
    return clause

def gap_fill_partition(min_id, max_id, partition_size, row_count):
    """ Adjust the partition size to accomodate for sparsity in the parittion key values"""
    fullrange = float(max_id) - float(min_id)
    sparsity = fullrange - row_count
    if sparsity < 1000:
        return partition_size
    multiple = math.ceil(fullrange/row_count)
    new_part = partition_size * multiple
    logger.debug(f"New partition size {new_part}")
    return new_part

def load_aggregate_data(ctx: Context, table: Table):
    logger.debug(f"Loading aggregate data for {table.name}")

    results = execute_sql_scalar(ctx, table.agg_sql, value_only=False)
    for key, value in results.items():
        agg_func, colname = get_col_and_aggfunc(key)
        column = get_from_list(table.columns, colname)
        if hasattr(column, "aggregations"):
            column.aggregations.update({agg_func: value})
        else:
            setattr(column, "aggregations", {agg_func: value})
        logger.debug(f"{table.full_name}.{column.name}: {column.aggregations}")
        
    logger.debug(f"{table.full_name}: {results}")

def generate_select_queries(min_id: int, max_id: int, partition_size:int, table : Table,
                            fields:list = [], clauses:list = []):
    """ A function that takes a min id, max id, partition size, table name, list of fields and list of clauses 
    and generates a list of SQL queries selecting all records from a table, where each query selects one parition
    
    Args:
        min_id (int): _description_
        max_id (int): _description_
        partition_size (int): _description_
        table_name (str): _description_
        fields (list): _description_
        clauses (list): _description_
    """
    #queries = []
    logger.info(f"generate_select_queries: min_id = {min_id}, max_id = {max_id}")
    queries_obj = IterableWrapper([])
    
    # Set the intitial min and max ids
    query_max_id = min_id + partition_size
    query_min_id = min_id
    i=0
    
    # When the query min id is > than the max_id, then all partitions are complete
    while query_min_id < max_id:  
        query = "SELECT "
        if not fields:
            query += " * "
        else:
            fields = []
            for field in fields:
                fields.append(field)
                
            query += ", ".join(fields)
        
        
        query += " FROM " + table.name + " WHERE id >= " + str(query_min_id) + \
                 " AND id < " + str(query_max_id)
                
        for clause in clauses:
            query += " AND " + clause
        
        #queries.append(query)
        queries_obj.append(query, {"table": table, "min_id": query_min_id, "max_id": query_max_id})
        
        # Update counters for next ierations 
        query_min_id = (min_id + i) * partition_size
        query_max_id = (min_id + i + 1) * partition_size
        i += 1
    # for i in range(math.ceil((max_id - min_id) / partition_size)):  
    #     query = "SELECT "
    #     if not fields:
    #         query += " * "
    #     else:
    #         fields = []
    #         for field in fields:
    #             fields.append(field)
                
    #         query += ", ".join(fields)
        
    #     query_min_id = (min_id + i) * partition_size
    #     query_max_id = (min_id + i + 1) * partition_size
    #     query += " FROM " + table.name + " WHERE id >= " + str(query_min_id) + \
    #              " AND id < " + str(query_max_id)
                
    #     for clause in clauses:
    #         query += " AND " + clause
        
    #     #queries.append(query)
    #     queries_obj.append(query, {"table": table, "min_id": query_min_id, "max_id": query_max_id})
        
    return queries_obj

###########################################################################################################################
# METADATA UTILITY FUNCTIONS
###########################################################################################################################
def list_agg_funcs_on_column(ctx: Context):
    for table in ctx.metadata.get_tables():
        for column in table.columns:
            if hasattr(column, "agg_funcs"):
                logger.debug(f"{column.name} agg funcs = {column.agg_funcs}")
 
def get_aggregagate(table: Table, col_name: str, agg_func_name: str):
    
    column = get_from_list(table.columns, col_name)
    if hasattr(column, "aggregations"):
        if agg_func_name in column.aggregations:
            return column.aggregations[agg_func_name]
        else:
            return None
    else:
        return None
    
def generic_repr(object):

    return "<{klass} @{id:x} {attrs}>".format(
            klass=object.__class__.__name__,
            id=id(object) & 0xFFFFFF,
            attrs=" ".join("{}={!r}".format(k, v) for k, v in object.__dict__.items()),
            )

class GeneralObj():
    """
    Instance, Timestamp, Database, TABLE_SCHEMA,TABLE_NAME,COLUMN_NAME,DATA_TYPE,CHARACTER_MAXIMUM_LENGTH,
    UNIQUE_COLUMN,IS_NULLABLE,PRIMARY_KEY,IS_IDENTITY,Index_Name,Ordinal_Position,Non_Unique,FULL_NAME

    """
    def __init__(self, attributes: dict) -> None:
        for i in attributes.items():
            setattr(self, i[0], i[1])

    def __repr__(self) -> str:
        return generic_repr(self)
    
class IterableWrapper(Iterable):
    """ Simple wrapping class for an iterator and a set of attribute

    Args:
        iterator (Iterable): iterator to wrap
        attributes (dict): dictionary of attributes to return with each iteration
    """
    def __init__(self, iterator: Iterable, attributes: dict = {}) -> None:
        self.iterator = iterator
        self.attributes = attributes
            
    def __iter__(self):
        for i in self.iterator:
            yield (i, self.attributes,)
            
    def append(self, obj: object, attributes: dict) -> None:
        self.iterator.append((obj, attributes,))

###########################################################################################################################
# BASE FUNCTIONS
###########################################################################################################################

def lookup_function(function_name: str):
    func = globals().get(function_name)
    if not func:
        func = locals().get(function_name)
    return func

def get_object(obj, name):
    """
    Determines how to access an object depending on it's type.
    getattr is the default. 
    get is used for dicts. 
    base.from_list() is used for lists, which assumes that all objects in the list have a name field.

    """
    if isinstance(obj, (dict,)):
        #logging.debug(f"Obj {obj} of type {type(obj)} is a dict")
        return obj.get(name)
    
    if isinstance(obj, (list,)):
        #print(f"Obj {obj} of type {type(obj)} is an list")
        return base.get_from_list(obj,name)

    if isinstance(obj, object):
        #print(f"Obj {obj} of type {type(obj)} is an object")
        if hasattr(obj, name):
            return getattr(obj, name)
        else:
            return None


###########################################################################################################################
# SAVE FUNCTIONS
###########################################################################################################################

def save_object(config: Configuration, instance, klass): # -> Integer
    
    try:
        engine = get_alchemy_engine(config, config.target_server)
        
        # TODO - this should be moved out to a devops process
        klass.__table__.create(bind=engine, checkfirst=True)
        
        with Session(engine) as session:
            session.add(instance)
            session.commit()

            return instance.id
    except Exception as e:
        logger.error(f"Error saving object {klass}:{instance}: {e}")
        return None 

def update_job_error(job_id, error_message):
    try:
        engine = get_alchemy_engine(config, config.target_server)
        
        # TODO - this should be moved out to a devops process
        klass.__table__.create(bind=engine, checkfirst=True)
        
        with Session(engine) as session:
            session.query(Job).filter(Job.id == job_id).update({"HasErrors": 1, "ErrorMessage": error_message})
            session.commit()
    except Exception as e:
        logger.error(f"Error saving error message for job {job_id}: {e}")
        return None 

def save_pandas_dataframe(ctx: Context, df: pd.DataFrame, name):

    engine = get_alchemy_engine(ctx.config, config.target_server)
 
    df.to_sql(
        name = name,
        con = engine
    )
    
def save_table_stats(ctx: Context):

    only_inclusion_tables = ctx.config.only_inclusion_tables
    table_inclusions = ctx.config.table_inclusions

    job_id = ctx.job_id

    db = ctx.metadata.get_db()
    database_name = db.name

    for schema in db.schemas:
        schema_name = schema.name

        for table in schema.tables:

            ts = jobs.TableStats()
            ts.job_id = job_id
            ts.database_name = database_name
            ts.schema_name = schema_name
            ts.table_name = table.name
            ts.field_count = len(table.columns)
            # ts.full_name = ?
            ts.row_count =  table.row_count

            included = 1
            if only_inclusion_tables:
                if table.name not in table_inclusions:
                    included = 0

            ts.included = included

            logger.info(f"Saving table stats for table {ts.table_name}")

            save_object(ctx.config, ts, jobs.TableStats)

def save_table_schema(ctx: Context, table: Table):

    logger.info(f"Saving table schema for table {table.name}")

    job_id = ctx.job_id

    for col in table.columns:
        ts = jobs.TableSchema()
        ts.job_id = job_id
        ts.db_name = getattr(col, 'database_name', None)
        ts.schema_name = getattr(col, 'schema_name', None)
        ts.table_name = getattr(col, 'table_name', None)
        ts.column_name = getattr(col, 'name', None)
        ts.data_type = getattr(col, 'data_type', None)
        ts.character_maximum_length = getattr(col, 'character_maximum_length', None)
        ts.UNIQUE_COLUMN = getattr(col, 'UNIQUE_COLUMN', None)
        ts.is_nullable = getattr(col, 'is_nullable', None)
        ts.primary_key = getattr(col, 'primary_key', None)
        ts.is_identity = getattr(col, 'is_identity', None)
        ts.index_name = getattr(col, 'index_name', None)
        ts.ordinal_position = getattr(col, 'ordinal_position', None)
        ts.non_unique = getattr(col, 'non_unique', None)
        ts.min = getattr(col, 'min', None)
        ts.max = getattr(col, 'max', None)
        ts.avg = getattr(col, 'avg', None)
        ts.stddev = getattr(col, 'stddev', None)
        ts.distinctcount = getattr(col, 'distinctcount', None)
        ts.strat_names = getattr(col, 'strat_names', None)

        save_object(ctx.config, ts, jobs.TableSchema)
        
def save_histogram(ctx: Context, table: Table, df: pd.DataFrame, field):

    logger.info(f"Saving table {table.name}_{field}_Histogram")

    df.sort_index()
    counter = 0
    for index, row in df.iterrows():

        hist = jobs.Histogram()

        hist.job_id = ctx.job_id
        hist.db_instance = ctx.config.src_server
        hist.db_name = ctx.metadata.get_db().name
        hist.table_name = table.name
        hist.field_name = field

        hist.bucket_number = counter
        hist.minval = index.left
        hist.maxval = index.right
        hist.count = row["count"]

        save_object(ctx.config, hist, jobs.Histogram)

        counter += 1

def save_topdistinct(ctx: Context, table: Table, df: pd.DataFrame, field):

    logger.info(f"Saving table {table.name}_{field}_TopCounts")

    df.sort_values(by=["count"])
    rank = 1
    for index, row in df.iterrows():

        top = jobs.TopCounts()

        top.job_id = ctx.job_id
        top.db_instance = ctx.config.src_server
        top.db_name = ctx.metadata.get_db().name
        top.table_name = table.name
        top.field_name = field
        top.rank = rank
        top.value = index
        top.count = row["count"]

        save_object(ctx.config, top, jobs.TopCounts)

        rank += 1