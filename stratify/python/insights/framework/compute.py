from insights.model.metadata import Table, Column
from insights.model.config import Context, Computation, Histogram
import pandas as pd 
from insights.util.base import get_from_list
import logging
import insights.util.base as base
import traceback

from loguru import logger

# # logger.remove()
# # logger.add(sys.stderr, level="DEBUG")

# compute_logger = logging.getLogger(__name__)
# compute_logger.setLevel(logging.DEBUG)

def histogram_prestep(ctx: Context, table: Table, computation: Computation):
    """ Calculate the bins for the histogram computation. This updates Column.computations["histogram"]
    """
    logger.debug(f"Histogram pre-step on {computation.name}")
    for column in table.columns:
        if hasattr(column, "computations"):
            histogram = get_from_list(column.computations, "histogram")
            if histogram:
                col_min = base.get_aggregagate(table, column.name, "min")
                col_max = base.get_aggregagate(table, column.name, "max")
                bins =  base.get_bins(col_min, col_max, float(histogram.bin_count), histogram.binning_algo)
                histogram.bins = bins
    
def histogram(ctx: Context, df: pd.DataFrame, table: Table, column_name: str, computation, start_id:int, end_id:int):
    logger.debug(f"Starting histogram on {table.name}_{column_name} with computation: {computation}")
    column = get_from_list(table.columns, column_name)
    col_series = df[column_name]
    partition_key = base.get_partition_key(ctx, table)
    column_segment_name = f"{start_id}_{end_id}_{partition_key}"
    col_histogram_obj = get_from_list(column.computations, "histogram") 

    hist = pd.DataFrame()
    try:
        if col_series.count() > 0:

            hist = pd.cut(col_series, bins=col_histogram_obj.bins).value_counts().rename(column_segment_name)
            return hist
        else:
            logger.debug(f"{column_name} has no non-NA values")
    except Exception as exc:
        logger.error(f"Error calculating histogram on {column_name}")
        logger.error(traceback.print_exc())
    
    return hist

def histogram_reduce(df: pd.DataFrame, computation):
    histogram_reduced = pd.DataFrame(df).sum(axis=1).to_frame(name="count")
    return histogram_reduced
 
def average(ctx: Context, df: pd.DataFrame, table: Table, column_name: str, computation, start_id:int, end_id:int):
    logger.debug(f"Starting average on {table.name}_{column_name} with computation: {computation}")
    col_series = df[column_name] # ->pd.Series
    partition_key = base.get_partition_key(ctx, table)
    column_segment_name = f"{start_id}_{end_id}_{partition_key}"

    avg = pd.DataFrame
    try:
        if col_series.count() > 0:
            avg = col_series.mean()
            count = col_series.count()
            avg = pd.DataFrame(index=["average", "count"], data=[avg,count], columns=[column_segment_name])
        else:
            logger.debug(f"{column_name} has no non-NA values")
    except Exception as exc:
        logger.error(f"Error calculating average on {column_name}")
        logger.error(traceback.print_exc())
    
    return avg

def average_reduce(df: pd.DataFrame, compute):
    total_count = pd.DataFrame(df).loc["count"].sum()
    split_totals = df.prod()
    total = split_totals.sum()
    average = total / total_count
    average_reduced = pd.DataFrame(index=["average", "count"], data=[average,total])
    return average_reduced

def topdistinct(ctx: Context, df: pd.DataFrame, table: Table, column_name: str, computation, start_id:int, end_id:int):
    logger.debug(f"Starting topdistinct on {table.name}_{column_name} with computation: {computation}")
    col_series = df[column_name] # ->pd.Series
    partition_key = base.get_partition_key(ctx, table)
    column_segment_name = f"{start_id}_{end_id}_{partition_key}"

    top = pd.DataFrame()
    try:
        if col_series.count() > 0:
            top = col_series.value_counts().rename(column_segment_name)
            return top
        else:
            logger.debug(f"{column_name} has no non-NA values")
    except Exception as exc:
        logger.error(f"Error calculating topdistinct on {column_name}")
        logger.error(traceback.print_exc())
    
    return top

def topdistinct_reduce(df: pd.DataFrame, compute):
    total_value_counts = pd.DataFrame(df).sum(axis=1)
    n = compute.top_count
    topdistinct_reduced = total_value_counts[:n].rename("count")
    return topdistinct_reduced 

def min(ctx: Context, df: pd.DataFrame, table: Table, column_name: str, computation, start_id:int, end_id:int):
    logger.debug(f"Starting min on {table.name}_{column_name} with computation: {computation}")
    col_series = df[column_name] # ->pd.Series
    partition_key = base.get_partition_key(ctx, table)
    column_segment_name = f"{start_id}_{end_id}_{partition_key}"

    mindf = pd.DataFrame()
    try:
        if col_series.count() > 0:
            minval = col_series.min()
            mindf = pd.DataFrame([minval], columns = [column_segment_name])
            return mindf
        else:
            logger.debug(f"{column_name} has no non-NA values")
    except Exception as exc:
        logger.error(f"Error calculating min on {column_name}")
        logger.error(traceback.print_exc())
    
    return mindf

def min_reduce(df: pd.DataFrame, compute):
    return df.min(axis=1).squeeze()

def max(ctx: Context, df: pd.DataFrame, table: Table, column_name: str, computation, start_id:int, end_id:int):
    logger.debug(f"Starting max on {table.name}_{column_name} with computation: {computation}")
    col_series = df[column_name] # ->pd.Series
    partition_key = base.get_partition_key(ctx, table)
    column_segment_name = f"{start_id}_{end_id}_{partition_key}"

    maxdf = pd.DataFrame()
    try:
        if col_series.count() > 0:
            maxval = col_series.max()
            maxdf = pd.DataFrame([maxval], columns = [column_segment_name])
            return maxdf
        else:
            logger.debug(f"{column_name} has no non-NA values")
    except Exception as exc:
        logger.error(f"Error calculating max on {column_name}")
        logger.error(traceback.print_exc())

    return maxdf

def max_reduce(df: pd.DataFrame, compute):
    return df.max(axis=1).squeeze()
