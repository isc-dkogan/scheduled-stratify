from insights.job.stratify import  stratify
from insights.model.config import Context, StratifyConfig


def test_stratify():
    config_dict = {
            "servers": [
                {"name": "local",
                "database": "User",
                "driver": "com.intersystems.jdbc.IRISDriver",
                "host": "localhost",
                "password": "sys",
                "user": "_SYSTEM",
                "port": 1972,
                "schemas": ["SQLUser"]
                },
                {"name": "b360",
                "database": "b360",
                "driver": "com.intersystems.jdbc.IRISDriver",
                "host": "localhost",
                "password": "test",
                "user": "superuser",
                "port": 1972,
                "schemas": ["halp_natixis"]
                }
            ],
            "run_type": "stratification",
            "table_exclusions": ["halp_natixis.harris_full_hist_aum_20230316",
                                    "halp_natixis.harris_aum_20230316",
                                    "halp_natixis.harris_full_hist_sales_20230316",
                                    "halp_natixis.harris_account_20230316"
            ],
            "table_inclusions": [
            ],
            "only_inclusion_tables": False,
            "strat_type_inclusion": [
                "integer",
                "double",
                "bigint",
                "numeric"
            ],
            "strat_type_mapping": [
                {
                    "type": "integer",
                    "name": "histogram",
                    "value": "$1",
                    "bin_count": 10
                } ,
                {
                    "type": "double",
                    "name": "histogram",
                    "value": "$1",
                    "bin_count": 10
                } ,
                {
                    "type": "bigint",
                    "name": "histogram",
                    "value": "$1",
                    "bin_count": 10
                } ,
                {
                    "type": "numeric",
                    "name": "histogram",
                    "value": "$1",
                    "bin_count": 10
                } ,
                {
                    "type": "varchar",
                    "name": "topdistinct",
                    "value": "$1",
                    "top_count": 10,
                    "max_unique": 100
                }
            ],
            "spark_jars": "/Users/psulin/projects/insights/bin/intersystems-jdbc-3.3.0.jar",
            "max_records_per_table": 0,
            "allocated_cpus": 7,
            "default_bins": 10,
            "src_server": "local",
            "partition_key": "ID",
            "allocated_memory_in_mb": 200,
            "random_sample_size": 1000,
            "sample_rate": 47,
            "conn_type": "odbc"
        }
    
    config = StratifyConfig(**config_dict)
    ctx = Context(config=config, job_id=-1)
    stratify(ctx)
    
if __name__ == "__main__":
    test_stratify()