import ray
from enum import Enum
import types
from typing import Iterable
from collections import defaultdict
import pandas as pd
 
import logging
#from loguru import logger
import time 
from functools import partial
import insights.util.base as base
from insights.model.config import Context
from insights.model.metadata import Table, Column
import insights.framework.compute as compute
import sys
from loguru import logger

# logger = logging.getLogger(__name__)
# logger.setLevel(logging.DEBUG)

# logger.remove()
# logger.add(sys.stderr, level="DEBUG")

def update_row_counts(ctx: Context):
    """Updates the row counts for all tables in the metadata

    Args:
        ctx (Context):  
    """
    def generator(ctx: Context):
        for table in ctx.metadata.get_tables():
            sql = f"SELECT COUNT(*) FROM {table.full_name}"
            yield (table.full_name, sql, )
    
    @ray.remote
    def apply(item, ctx, idx):
        sql = item[1]
        table_name = item[0]
        row_count = base.execute_sql_scalar(ctx, sql, value_only=True)[0]
        return (table_name, row_count,)
    
    def accumulate(full_results, result, task_data):
       full_results[result[0]] = result[1]
       return full_results
    
    results_name = "row_counts"
    partial_generator = partial(generator, ctx=ctx)
    dp = DataProcessor(name=results_name, context=ctx, split_func=partial_generator, apply_func=apply,
                       accumulate_func=accumulate, reduce_func=None, parallelization=2, results_type=int)
    
    # This is a synchronous call and the ctx is updated internally. This could be the reduce_func 
    # and could be added to the Data Processor.
    ctx = dp.run()
    results = getattr(ctx, results_name)
    for table in ctx.metadata.get_tables():
        table.row_count = results[table.full_name]
        logger.info(f"Table: {table.name}, Row Count: {table.row_count}")
        
    return ctx

def calculate_partition_min_max(ctx: Context, table: Table):
    """Updates the row counts for all tables in the metadata

    Args:
        ctx (Context):  
    """
    def split():

        part_size = 200
        row_count = ctx.row_counts[table.name]

        partition_key = base.get_partition_key(ctx, table)

        sql_partitions = base.IterableWrapper([])
    
        # Set the intitial min and max ids
        query_max_id = part_size
        query_min_id = 0
        i=0
        
        while query_min_id <= row_count:
            limit = part_size if query_max_id <= row_count else row_count - query_min_id
            query = "SELECT " + partition_key + " FROM " + table.name + " LIMIT " + str(limit) + " OFFSET " + str(query_min_id)

            sql_partitions.append(query, {"table": table, "min_id": query_min_id, "max_id": query_max_id})
            
            # Update counters for next ierations 
            query_min_id = i * part_size
            query_max_id = (i + 1) * part_size
            i += 1

        return sql_partitions
    
    @ray.remote
    def apply(item, ctx, idx):
        iter_obj = item[0]
        sql = iter_obj[0]
        attributes = iter_obj[1]
        
        table = attributes.get("table")
        min_id = attributes.get("min_id")
        max_id = attributes.get("max_id")

        connection = base.get_connection(ctx.config, ctx.conn_type, "")

        df = pd.read_sql(sql, connection)
        records = len(df)

        col = base.get_partition_key(ctx, table)
        
        partial_results = defaultdict(dict)

        min_compute_results = compute.min(ctx, df, table, col, None, min_id, max_id)
        if min_compute_results is not None and not min_compute_results.empty:
            try:
                partial_results[table.name]["min_partition_key"] = min_compute_results
            except Exception as ex:
                logger.error(f"Error calculating partial results for min{col} {ex}")

        max_compute_results = compute.max(ctx, df, table, col, None, min_id, max_id)
        if max_compute_results is not None and not max_compute_results.empty:
            try:
                partial_results[table.name]["max_partition_key"] = max_compute_results
            except Exception as ex:
                logger.error(f"Error calculating partial results for max{col} {ex}")

        connection.close()
        
        return partial_results
    
    def accumulate(full_results, result, task_data):

        for table_name, column_obj in result.items():
            for compute_name, partial_df in column_obj.items():
                if partial_df is not None:
                    try:
                        
                        table = base.get_object(ctx.partition_key_aggs, table_name)

                        if not table:
                            ctx.partition_key_aggs[table_name] = defaultdict(dict)
                            table = base.get_object(ctx.partition_key_aggs, table_name)
                            
                        if compute_name in table.keys():
                            existing_results = table.get(compute_name)
                            table[compute_name] = pd.concat([existing_results, partial_df], axis=1)
                        else:
                            table[compute_name] = partial_df
                        
                    except Exception as ex:
                        logger.error(f"ERROR adding partial results for {table_name} and {compute_name} {ex}")

        return ctx.partition_key_aggs

    logger.info(f"Updating partition min and max values for table {table}")

    results_name = "partition_key_aggs"
    dp = DataProcessor(name=results_name, context=ctx, split_func=split, apply_func=apply,
                       accumulate_func=accumulate, reduce_func=None, parallelization=2, results_type=int)
    
    # This is a synchronous call and the ctx is updated internally. This could be the reduce_func 
    # and could be added to the Data Processor.
    ctx = dp.run()
    results = getattr(ctx, results_name)

    table_results = results[table.name]
    table_results["min_partition_key"] = compute.min_reduce(table_results["min_partition_key"], None)
    table_results["max_partition_key"] = compute.max_reduce(table_results["max_partition_key"], None)
        
    return ctx

def calculate_aggregates(ctx: Context, table: Table):
    """Updates the row counts for all tables in the metadata

    Args:
        ctx (Context):  
    """
    def split():

        min_id = ctx.partition_key_aggs[table.name]["min_partition_key"]
        max_id = ctx.partition_key_aggs[table.name]["max_partition_key"]

        part_size = base.gap_fill_partition(min_id, max_id, 200, ctx.row_counts[table.name])

        sql_partitions = base.generate_select_queries(min_id=min_id, max_id=max_id, 
                                                partition_size=part_size, table=table) # -> base.IterableWrapper

        return sql_partitions
    
    @ray.remote
    def apply(item, ctx, idx):
        iter_obj = item[0]
        sql = iter_obj[0]
        attributes = iter_obj[1]
        
        table = attributes.get("table")
        min_id = attributes.get("min_id")
        max_id = attributes.get("max_id")

        connection = base.get_connection(ctx.config, ctx.conn_type, "")
        
        df = pd.read_sql(sql, connection)
        
        partial_results = defaultdict(dict)

        for column in table.columns:
            for datatype_map in ctx.datatype_mapping:
                if base.clean_data_type(column.data_type) in datatype_map.subtypes:

                    if hasattr(column, 'agg_funcs'):
                        column.agg_funcs.extend(datatype_map.agg_funcs2)
                    else:
                        setattr(column, 'agg_funcs', datatype_map.agg_funcs2)
            if hasattr(column, "agg_funcs"):
                for agg in column.agg_funcs:
                    compute_func = base.lookup_function(agg)
                    if not compute_func:
                        raise Exception(f"Compute function {agg} not found")
                    
                    compute_results = compute_func(ctx, df, table, column.name, None, min_id, max_id)

                    if compute_results is not None and not compute_results.empty:
                        try:
                            partial_results[agg][f"{table.name}.{column.name}"] = compute_results
                        except Exception as ex:
                            logger.error(f"Error calculating partial results for {agg} {column.name} {ex}")
        
        return partial_results
    
    def accumulate(full_results, result, task_data):

        for compute_name, column_obj in result.items():
            for column_name, partial_df in column_obj.items():
                if partial_df is not None:
                    try:
                        compute = base.get_object(ctx.results, compute_name)
                        if not compute:
                            ctx.results[compute_name] = defaultdict(dict)
                            compute = base.get_object(ctx.results, compute_name)
                        if column_name in compute.keys():
                            existing_results = compute.get(column_name)
                            compute[column_name] = pd.concat([existing_results, partial_df], axis=1)
                        else:
                            compute[column_name] = partial_df
                    except Exception as ex:
                        logger.error(f"ERROR adding partial results for {compute_name} and {column_name} {ex}")

        return ctx.results

    def reduce(df, compute_name, table_col):
        if table_col.split("*")[0] == table.name:
            logger.debug(f"Reducing results for {compute_name} of {table_col}")
            reduce_function = base.lookup_function(f"{compute_name}_reduce")
            if not reduce_function:
                raise Exception(f"Reduce function {compute_name} not found")

            agg_val = reduce_function(df, None)
            column = base.get_from_list(table.columns, table_col.split("*")[1])
            if hasattr(column, "aggregations"):
                column.aggregations.update({compute_name: agg_val})
            else:
                setattr(column, "aggregations", {compute_name: agg_val})

            
            ctx.results = {}

    logger.info(f"Calculating aggregate values for table {table}")

    results_name = "aggregations"
    dp = DataProcessor(name=results_name, context=ctx, split_func=split, apply_func=apply,
                       accumulate_func=accumulate, reduce_func=reduce, parallelization=2, results_type=int)
    
    # This is a synchronous call and the ctx is updated internally. This could be the reduce_func 
    # and could be added to the Data Processor.
    ctx = dp.run()
        
    return ctx

class ActorTypes(Enum):
    SQL = 0
     
class TaskData():
    
    def __init__(self, start_index, iter_obj = None, data: dict = None) -> None:
        self.start_index = start_index
        self.iter_obj = iter_obj
        self.data = data
        
    def __repr__(self) -> str:
        return f"{self.start_index}|{self.iter_obj}|{self.data}"

class  DataProcessor():
    """ DataProcessor is the base class for a distributed data processing task that can provide one component of distributed
        workflow. It provides fan-out, round robin parallel task execution that uses split/partition, apply, accumulate, reduce
        reduce structure to provide in-memory, dynamically scalable multinode processing.
        It requires an Iterator object for split/partitioning, a function to apply to each partition, 
        a function to accumulate the results from the apply function, and a function to reduce the results
        to their final form. Additionally, pre-processing and post-processing steps can be provided which will
        be executed after the denoted step, pre-split, post-split, pre-apply, post-apply, etc...
    """
    def __init__(self, name: str, context: Context, split_func: types.FunctionType = None, 
                 apply_func: types.FunctionType = None, accumulate_func:types.FunctionType = None,
                 reduce_func:types.FunctionType = None, parallelization: int = 1, results_type = None,
                 actor_cls: object = None):
        self.name = name
        self.ctx = context
        if split_func:
            self.split = split_func 
        if apply_func:
            self.apply = apply_func 
        if accumulate_func:
            self.accumulate = accumulate_func 
        if reduce_func:
            self.reduce = reduce_func
        self.parallelization = parallelization
        self.results_type = results_type
        self.actor_cls = actor_cls
        
        # self.logger = logger
        # self.logger.remove()
        # self.logger.add(sys.stderr, level="DEBUG")
    
    def run(self):
    
        self.ctx = self.execute_tasks()
        return self.ctx
        
    def execute_tasks(self):
        """_summary_
        Distributes tasks in a fan out manner, running up to X tasks set by parallelism.
        The key(pun intended) to accumulating the results and then being able to reduce them later
        is the mapping in the full_results object. The accumulator and reduce function(reduce is not part of this method)
        rely on having a common understanding of how each piece of the full_results is keyed.
        
        Args:
            iterator (Iterable): _description_
            function (types.FunctionType): _description_
            parallelism (int): _description_
            accumulator (types.FunctionType, optional): _description_. Defaults to None.
            ctx (dict, optional): _description_. Defaults to None.
            name: (str) give a name to the task set
            results_type: creates a default type for the dict so no initialization is needed in accumulator
            pretask_steps (list, optional): Steps to execute before starting iteration. Defaults to None.
            posttask_steps (list, optional): Steps to execute after iteration. Defaults to None.
            use_actors (bool, optional): Use Ray Actors. Defaults to False.
            actor_cls (type, optional): The actor class to use. Defaults to None for tasks

        Returns:
            _type_: _description_
        """
        logger.info("starting execute_tasks")
        total_tasks_started = total_tasks_completed = current_tasks = 0
        executing_task_refs = []
        ref_tracker = {}
        full_results = {} if self.results_type is None else defaultdict(self.results_type)
        
         # We create one actor per parallel task and then use the counter
        actors = []
        if self.actor_cls is not None:
            for i in range(0, self.parallelization):
                actor_ref = self.actor_cls.remote(self.ctx)
                actors.append(actor_ref)
        
        # Can be defined as a function or Iterable class. If it is an iterable class, it will already
        # have a reference to the ctx
        iterator = self.split() if callable(self.split) else self.split
        for idx, item in enumerate(iterator,1):
            
            if idx % 1000 == 0:
                logger.info(f"{idx} iters started")
                
            logger.info(f"ITEM ITERATION {idx}: type(item): {type(item)}")
            # Use the idx of the task or a name attribute on the iterator object as it's identity
            id = getattr(item, "name", None) or idx
            
            # Execute remote task
            if self.actor_cls:
                logger.debug("Getting actor")
                # TODO - Change this to use an actor pool. The logic of mod'ing on idx will not work for this case
                # actor = actors[idx % self.parallelization] 
                actor = self.actor_cls.remote(self.ctx)
                task_ref = actor.apply.remote(item, idx)
            else:
                task_ref = self.apply.remote(item, self.ctx, idx)
                
            logger.debug(f"task_ref: {task_ref}")
            
            # Track the executing_task_refs, which is modified later by removing completed tasks
            executing_task_refs.append(task_ref)
            
            # Use the unique id of the task_ref to store data about the task that is required for the accumulator
            ref_tracker[task_ref] = TaskData(idx, item)
            
            # Update Counters
            total_tasks_started += 1
            current_tasks += 1

            # If desired parallelism has been reached, wait until some have completed and collect results
            if current_tasks % self.parallelization == 0:
                # blocking
                logger.debug(f"Full parallelization, waiting on {len(executing_task_refs)}")
                ready, working = ray.wait(executing_task_refs)
                results = ray.get(ready)
                results = results if type(results) is list else [results]
                #logger.debug(f"results - {results}")
                # results is a parallel list to ready. ready contains the task_ref, used as key for task data in ref_tracker 
                # and results contains return values for the task
                for idx_results, result in enumerate(results):
                    #logger.debug(f"results[{idx_results}]: {result}")
                    remote_ref = ready[idx_results]
                    task_data = ref_tracker[remote_ref]
                    full_results = self.accumulate(full_results, result, task_data)
                    
                    # Return actor back to pool
                    # if self.actor_pool:
                    #     self.actor_pool.put(remote_ref)
                        
                    #logger.debug(f"Full Results: {full_results}")
                executing_task_refs = working
                completed = len(ready)
                current_tasks -= completed
                total_tasks_completed += completed
                logger.debug(f"Still in Iteration, completed {completed} of {total_tasks_completed} total completed, waiting on {len(working)}")

        # Get any remaining tasks. This will always be a max of the parallelization since during the iteration loop
        # there is a blocking wait based on the mod of the parallelization
        logger.debug(f"Completed loop, still waiting on {len(executing_task_refs)}")
        ready, working = ray.wait(executing_task_refs, num_returns=len(executing_task_refs))
        results = ray.get(ready)
        for idx_final_results, result in enumerate(results):
            task_data = ref_tracker[ready[idx_final_results]]
            full_results = self.accumulate(full_results, result, task_data)
            #print(f"Full Results: {full_results}")
                    
        if working: logger.error("Tasks still pending, should not happen")
        completed = len(ready)
        current_tasks -= completed
        total_tasks_completed += completed
        logger.info(f"Out of iteration, completed {completed} more of {total_tasks_completed} total completed, waiting on {len(working)}")

        # Iterate through full_results and apply appropriate _reduce function
        logger.info(f"Full Results has len {len(full_results)}")

        # for key, value in base.get_object(self.ctx, "results").items():
        for key, value in full_results.items():
            if (self.name != "row_counts") and (self.name != "partition_key_aggs"):
                for table_col, df in value.items():
                    self.reduce(df, key, table_col)
            
        setattr(self.ctx, self.name, full_results)
        return self.ctx

    def read_sql_with_retries(self, sql, retries: int):
        """
        The function `read_sql_with_retries` reads SQL data from a database connection with retries in case
        of failure.
        
        :param sql: The `sql` parameter is a string that represents the SQL query you want to execute and
        retrieve data from. It is used as an argument for the `pd.read_sql()` function
        :param retries: The "retries" parameter is an integer that specifies the number of times the code
        should retry reading the SQL query in case of failure
        :type retries: int
        :return: a pandas DataFrame object.
        """
        
        df = None 
        try:
            df = pd.read_sql(sql, self.connection)
        except Exception as exc:
            logger.warning(f"failed to read {sql}, possibly no records or {self.conn_type} failed. Trying with other connection type")
            logger.warning(exc)
            try:
                logger.info("Taking a short nap, then trying again")
                time.sleep(2)
                conn_type = self.config.get("strat_retry_conn_type")
                connection = base.get_connection(self.config, self.instance, conn_type)
                
                df = pd.read_sql(sql, connection)
                logger.info(f"Suceeded with {conn_type}")
            except Exception as ex:
                logger.error(f"Failed to read with {conn_type}: {sql}") 
                logger.warning(ex)
        
        return df
    
    
