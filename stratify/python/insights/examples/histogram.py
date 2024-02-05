from insights.model.config import Context, Configuration
from insights.model.metadata import Table 
from loguru import logger 

import sys 
import ray 
import insights.util.base as base
import insights.util.core as core
from insights.framework.compute import *
from collections import defaultdict
import pandas as pd
import time 


@ray.remote
class AvgDataProcessor(core.DataProcessor):
    
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
        
    def split(self):
        """ Generate SQL SELECT statements of equal partitions of records to be loaded and operated on by the apply function
        """
        
        part_size = base.gap_fill_partition(self.min_id, self.max_id, self.partition_size, self.table.row_count)
        logger.info(f"New Part size {part_size}")
        sql_partitions = base.generate_select_queries(min_id=self.min_id, max_id=self.max_id, 
                                                partition_size=part_size, table=self.table) # -> base.IterableWrapper

        logger.info(f"Generated {len(sql_partitions.iterator)} SQL queries")
        return sql_partitions
        #return base.IterableWrapper(iterator = sql_partitions, attributes= {"min_id": self.min_id, "max_id": self.max_id})

    def accumulate(self, full_results, result, task_data):
       #logger.debug(f"Full Results: {full_results}, result: {result})")
        
        #print(f"Accumulator: {full_results}, result: {result}")
        #{'histogram': {'ID': None, 'EndOfDayPrice': None, 'MarketValue': None, 'PreviousEndOfDayPrice': None, 'Quantity': None, 'StartOfDayPosition': None,
        # 'StartOfDayPrice': None, 'TradeSettleAmount': None}, 'topdistinct': {'PositionCurrency': None, 'SecurityID': None, 'TradingBook': None, 'createUser': None, 'updateUser': None}})
        
        for compute_name, column_obj in result.items():
            for column_name, partial_df in column_obj.items():
                if partial_df is not None:
                    try:
                        print(f"Adding partial results for {compute_name} and {column_name} with {len(partial_df)} rows")
                        # Get or create the DF that will accumulate results
                        # if hasattr(self.ctx, compute_name):
                        #     print(f"Compute with name {compute_name} already exists {getattr(self.ctx, compute_name)}")
                        # else:
                        #     setattr(self.ctx, compute_name, {})
                        try:
                            compute = getattr(self.ctx, compute_name)
                            print(f"Got compute: {compute.keys()}")
                        except:
                            print(f"Failed getting attribute for {compute_name}, setting new one")
                            setattr(self.ctx, compute_name, defaultdict(dict))
                            compute = getattr(self.ctx, compute_name)
                            
                        # print(f"Compute to add results to {compute}")
                        # If it alread exists, concat to existing df
                        if column_name in compute.keys():
                            existing_results = compute.get(column_name)
                            print(f"Concating {column_name}")
                            compute[column_name] = pd.concat([existing_results, partial_df], axis=1)
                        else:
                            print(f"Creating new {column_name}")
                            compute[column_name] = partial_df
                            
                        # concat the new results to this DF
                        #full_results[compute_name][column_name] += partial_df
                    except Exception as ex:
                        print(f"ERROR adding partial results for {compute_name} and {column_name} {ex}")

                    print()
        return full_results
 
@ray.remote
class AvgApply(object):
    """ DBActor is a Ray actor that connects to a database and executes a query on it and returns the results in a dataframe."""
    
    def __init__(self, ctx: Context, instance: str = "") -> None:
        self.ctx = ctx
        self.conn_type = ctx.conn_type
        self.instance = instance if instance!= "" else ctx.src_server
        self.config = ctx.config
        self.connection = base.get_connection(self.config, self.conn_type, instance)
        

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
        print(f"Read {records} rows from {sql}")
        
        partial_results = defaultdict(dict)
        
        for computation in table.computations:
            compute_name = computation.name
            compute_func = base.lookup_function(compute_name)
            if not compute_func:
                print(f"DID NOT FIND Compute function for {compute_name} compute_func = {compute_func}")
                raise Exception(f"Compute function {compute_name} not found")
            
            for col in computation.columns:
                compute_results = compute_func(self.ctx, df, table, col, computation, min_id, max_id)
                if table.name == "Position" and col == "ID":
                    print(f"{min_id} to {max_id} for Position ID")
                if compute_results is not None and not compute_results.empty:
                    try:
                        partial_results[compute_name][f"{table.name}_{col}"] = compute_results
                        print(f"Partial Results for {table.name}_{col} : {len(compute_results)}")
                    except:
                        print("Error calculating partial results for {compute_name}{col} {traceback.print_exc()}")
        
        return partial_results
  
          
def run(ctx):
     # Get metadata for schemas
    ctx.metadata = base.get_metadata(ctx)
    
    # Load Row Counts
    ctx = core.update_row_counts(ctx)
    
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
        
        # Check that the table is not excluded in the config
        if ctx.config.only_inclusion_tables & (table.name not in ctx.table_inclusions):
            continue
        
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
        logger.debug(f"{table.name} - min_id: {min_id}, max_id: {max_id}")
        
        dp = AvgDataProcessor.remote(name=table.full_name, context=ctx, parallelization=4, results_type=int, 
                                     actor_cls=AvgApply, table=table, min_id=min_id, max_id=max_id,
                                     partition_size=200)
    
        ref = dp.run.remote()
        ctx = ray.get(ref)
        time.sleep(2)
        print(f"Completed processing table {table.name}: {ref}")
        
        print(f'histogram of Position ID \n{ctx.histogram["SecurityMaster_Coupon"]}')
        histogram_reduced = ctx.histogram["SecurityMaster_Coupon"].sum()
        print(f'histogram of Position ID \n{histogram_reduced}')
        
        # Save the histogram_reduced
        base.save_pandas_dataframe(ctx, histogram_reduced, "SecurityMaster_Coupon_Histogram")
        
    """
    """
        

    
if __name__ == '__main__':
    
    config_path = sys.argv[1]
    
    config = Configuration.parse_file(config_path)
    logger.debug(f"Config: {config}")

    # TODO - add job creation to get job_id
    ctx = Context(config=config, job_id = -9)
    
    run(ctx)
    
   