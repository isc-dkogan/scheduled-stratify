numeric_types = ["bigint", "int", "integer", "smallint", "tinyint", "bit", "decimal", 
                "numeric", "float", "double"]

dtype_sizes = {"bigint": 8, "int": 4, "smallint": 2, "tinyint": 1, "bit": 1, "decimal": 8,
                "numeric": 8, "float": 8, "date": 4, "timestamp": 8, "varchar": 8}

numeric_agg_funcs = ["avg($1)", "min($1)","max($1)", "stddev($1)"] 
varchar_agg_funcs =  ["COUNT(DISTINCT($1))"]