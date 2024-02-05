from insights.model.config import Context, Configuration
from insights.model.metadata import Table 
#from loguru import logger
import sys 
import ray 
import insights.util.base as base
import insights.util.core as core
from insights.framework.compute import *
from collections import defaultdict
import pandas as pd
import time 

# logger = logging.getLogger(__name__)
# logger.setLevel(logging.DEBUG)

# logger.remove()
# logger.add(sys.stderr, level="DEBUG")

@ray.remote
class SQLToDataFrameDataProcessor(core.DataProcessor):
    
    def __init__(self, name: str, context: Context, table: Table, min_id: int, max_id: int, 
                 partition_size: int, parallelization: int = 1, results_type = None,
                 actor_cls: object = None, fields: list = [], clauses: list = []):
        super().__init__(name=name, context=context, actor_cls=actor_cls)
        self.name = name
        self.ctx = context
        self.results_type = results_type
        self.parallelization=parallelization
        self.actor_cls = actor_cls
        self.table = table
        self.min_id = min_id
        self.max_id = max_id
        self.partition_size = partition_size
        self.fields = fields
        self.clauses: clauses
        
        from loguru import logger

        self.logger = logger
        self.logger.remove()
        self.logger.add(sys.stderr, level="DEBUG")
        
    def split(self):
        """ Generate SQL SELECT statements of equal partitions of records to be loaded and operated on by the apply function
        """
        
        part_size = base.gap_fill_partition(self.min_id, self.max_id, self.partition_size, self.table.row_count)
        self.logger.info(f"New Part size {part_size}")
        sql_partitions = base.generate_select_queries(min_id=self.min_id, max_id=self.max_id, 
                                                partition_size=part_size, table=self.table) # -> base.IterableWrapper

        self.logger.info(f"Generated {len(sql_partitions.iterator)} SQL queries")
        return sql_partitions
        #return base.IterableWrapper(iterator = sql_partitions, attributes= {"min_id": self.min_id, "max_id": self.max_id})

    def accumulate(self, full_results, result, task_data):
       #logger.debug(f"Full Results: {full_results}, result: {result})")
        
        # print(f"Accumulator: {full_results}, result: {result}")
        #{'histogram': {'ID': None, 'EndOfDayPrice': None, 'MarketValue': None, 'PreviousEndOfDayPrice': None, 'Quantity': None, 'StartOfDayPosition': None,
        # 'StartOfDayPrice': None, 'TradeSettleAmount': None}, 'topdistinct': {'PositionCurrency': None, 'SecurityID': None, 'TradingBook': None, 'createUser': None, 'updateUser': None}})

        for compute_name, column_obj in result.items():
            for table_col, partial_df in column_obj.items():
                if partial_df is not None:
                    try:
                        self.logger.debug(f"Adding partial results for {compute_name} and {table_col} with {len(partial_df)} rows")
                        
                        compute = base.get_object(self.ctx.results, compute_name)
                        if not compute:
                            self.ctx.results[compute_name] = defaultdict(dict)
                            compute = base.get_object(self.ctx.results, compute_name)
                            
                        # If it already exists, concat to existing df
                        if table_col in compute.keys():
                            existing_results = compute.get(table_col)
                            self.logger.debug(f"Concating {table_col}")
                            compute[table_col] = pd.concat([existing_results, partial_df], axis=1)
                        else:
                            self.logger.debug(f"Creating new {table_col}")
                            compute[table_col] = partial_df
                        
                        # concat the new results to this DF
                    except Exception as ex:
                        self.logger.error(f"ERROR adding partial results for {compute_name} and {table_col} {ex}")

        return self.ctx.results

    def reduce(self, df, compute_name, table_col):
        if table_col.split("*")[0] == self.table.name:
            self.logger.debug(f"Reducing results for {compute_name} of {table_col}")
            reduce_function = base.lookup_function(f"{compute_name}_reduce")
            if not reduce_function:
                self.logger.debug(f"DID NOT FIND Reduce function for {compute_name} reduce_func = {reduce_function}")
                raise Exception(f"Reduce function {compute_name} not found")
            
            results = base.get_object(self.ctx, "results")
            tables = self.ctx.metadata.get_tables()
            table = base.get_from_list(tables, table_col.split("*")[0])
            compute = base.get_from_list(table.computations, compute_name)

            results[compute_name][table_col] = reduce_function(df, compute)
 
@ray.remote
class Apply(object):
    """ DBActor is a Ray actor that connects to a database and executes a query on it and returns the results in a dataframe."""
    
    def __init__(self, ctx: Context, instance: str = "") -> None:
        self.ctx = ctx
        self.conn_type = ctx.conn_type
        self.instance = instance if instance!= "" else ctx.src_server
        self.config = ctx.config
        self.connection = base.get_connection(self.config, self.conn_type, instance)
        
        from loguru import logger
        self.logger = logger
        self.logger.remove()
        self.logger.add(sys.stderr, level="DEBUG")

    def apply(self, item, idx):
        
        iter_obj = item[0]
        sql = iter_obj[0]
        attributes = iter_obj[1]
        
        table = attributes.get("table")
        min_id = attributes.get("min_id")
        max_id = attributes.get("max_id")
        
        # TODO - Some minimal retry handling should be done for momentary issues
        #df = self.read_sql_with_retries(sql, 1)
        df = pd.read_sql(sql, self.connection)
        records = len(df)
        #print(f"iter_obj {iter_obj}")
        self.logger.debug(f"Read {records} rows from {sql}")
        
        partial_results = defaultdict(dict)
        
        for computation in table.computations:
            compute_name = computation.name
            compute_func = base.lookup_function(compute_name)
            if not compute_func:
                self.logger.error(f"DID NOT FIND Compute function for {compute_name} compute_func = {compute_func}")
                raise Exception(f"Compute function {compute_name} not found")
            
            for col in computation.columns:

                compute_results = compute_func(self.ctx, df, table, col, computation, min_id, max_id)

                if compute_results is not None and not compute_results.empty:
                    try:
                        partial_results[compute_name][f"{table.name}*{col}"] = compute_results
                        self.logger.debug(f"Partial Results for {computation.name} on {table.name}_{col} : {len(compute_results)}")
                    except:
                        self.logger.error(f"Error calculating partial results for {compute_name} {col}")

        self.connection.close()
        
        return partial_results
  
          
def run(ctx):
    
    from loguru import logger
    logger.remove()
    logger.add(sys.stderr, level="DEBUG")
    
     # Get metadata for schemas
    ctx.metadata = base.get_metadata(ctx)
    
    # Load Row Counts
    ctx = core.update_row_counts(ctx)

    ctx.partition_key_aggs = {}
    
    # Save to Table_stats
    base.save_table_stats(ctx)
    
    # Create the aggregation sql for each table and add it to the table metadata as agg_sql
    # This may only be necessary in order to get the min and max values for the partition key for each
    # table. These are required in order to split the queries into partitions.
    ctx = base.create_aggregate_sql(ctx, set_required_aggregates=base.datatype_aggregate_requirements)
    
    # Computations can have additional required steps. Histograms, for example, may need to calculate bins based
    # on the min max values for the respective column.
    ctx = base.create_computations(ctx)

    for table in ctx.metadata.get_tables():

        start = time.time()

        if table.row_count == 0:
            continue
        
        # Check that the table is not excluded in the config
        if ctx.config.only_inclusion_tables & (table.name not in ctx.table_inclusions):
            logger.info(f"Skipping table {table.name} because it is not included in the config")
            continue
        else:
            logger.debug(f"Processing table {table.name}")

        # Calculate partition key min and max
        # core.calculate_partition_min_max(ctx, table)

        # Calculate aggregates
        # core.calculate_aggregates(ctx, table)
        
        # Loading the aggregate data issues a potentially long running SQL to the database. This is done here, instead
        # of when the SQL is created, as it could be parallelized.
        base.load_aggregate_data(ctx, table)

        # Save to Table_schema
        base.save_table_schema(ctx, table)
        
        # Computations that have parameters that require aggregate data are updated or added
        base.update_computations(ctx, table)
        
        # Get required paramaters for AvgDataProcessor
        partition_key = base.get_partition_key(ctx, table)
        min_id = base.get_aggregagate(table, partition_key, "min")
        max_id = base.get_aggregagate(table, partition_key, "max")
        logger.warning(f"{table.name} - min_id: {min_id}, max_id: {max_id}")
        
        dp = SQLToDataFrameDataProcessor.remote(name=table.full_name, context=ctx, parallelization=4, results_type=int, 
                                     actor_cls=Apply, table=table, min_id=float(min_id), max_id=float(max_id),
                                     partition_size=100000)
    
        ref = dp.run.remote()
        ctx = ray.get(ref)

        elapsed = time.time() - start

        time.sleep(2)
        logger.info(f"Completed processing table {table.name}: {ref} in {elapsed} secs")

        results = base.get_object(ctx, "results")
  
        for key, value in results.items():
            for table_col, df in value.items():
                if table_col.split("*")[0] == table.name: 
                    logger.debug(f"Saving results for {table_col}")
                    save_function = base.lookup_function(f"save_{key}")
                    if save_function:
                        save_function(ctx, table, pd.DataFrame(df), table_col.split("*")[1])
                    else:
                        logger.error(f"DID NOT FIND Save function for {key}")
                
                
        time.sleep(2)
        
    """
    """
        

if __name__ == '__main__':
    
    from loguru import logger
    logger.remove()
    logger.add(sys.stderr, level="DEBUG")
    config_path = sys.argv[1]
    print(logger)
    config = Configuration.parse_file(config_path)
    logger.debug(f"Config: {config}")

    ctx = Context(config=config, job_id = -9)
    
    run(ctx)
    
   