import insights.util.utils as utils
from insights.model.config import StratifyConfig, Server
from insights.model.metadata import Table
import unittest
import insights.util.base as base
# def test_jdbc_connection():
    
#     config_json =  {
#     "servers": [
#       {"name": "local",
#       "database": "User",
#       "driver": "com.intersystems.jdbc.IRISDriver",
#       "host": "localhost",
#       "password": "SYS",
#       "user": "_SYSTEM",
#       "port": 1972,
#       "schemas": ["SQLUser"]
#       },
#       {"name": "b360",
#       "database": "Ub360ser",
#       "driver": "com.intersystems.jdbc.IRISDriver",
#       "host": "localhost",
#       "password": "test",
#       "user": "superuser",
#       "port": 1972,
#       "schemas": ["halp_natixis"]
#       }
#     ],
#     "run_type": "stratification",
#     "table_exclusions": ["halp_natixis.harris_full_hist_aum_20230316",
#                          "halp_natixis.harris_aum_20230316",
#                          "halp_natixis.harris_full_hist_sales_20230316",
#                          "halp_natixis.harris_account_20230316"
#     ],
#     "table_inclusions": [
#     ],
#     "only_inclusion_tables": False,
#     "strat_type_inclusion": [
#       "integer",
#       "double",
#       "bigint",
#       "numeric"
#     ],
#     "strat_type_mapping": [
#         {
#           "type": "integer",
#           "name": "histogram",
#           "value": "$1",
#           "bin_count": 10
#         } ,
#         {
#           "type": "double",
#           "name": "histogram",
#           "value": "$1",
#           "bin_count": 10
#         } ,
#         {
#           "type": "bigint",
#           "name": "histogram",
#           "value": "$1",
#           "bin_count": 10
#         } ,
#         {
#           "type": "numeric",
#           "name": "histogram",
#           "value": "$1",
#           "bin_count": 10
#         } ,
#         {
#           "type": "varchar",
#           "name": "topdistinct",
#           "value": "$1",
#           "top_count": 10,
#           "max_unique": 100
#         }
#     ],
#     "spark_jars": "/Users/psulin/projects/insights/bin/intersystems-jdbc-3.3.0.jar",
#     "max_records_per_table": 0,
#     "allocated_cpus": 7,
#     "default_bins": 10,
#     "src_server": "local",
#     "partition_key": "ID",
#     "allocated_memory_in_mb": 200,
#     "random_sample_size": 1000,
#     "sample_rate": 47
#     } 
#     config = StratifyConfig.parse_obj(config_json)
#     conn = utils.get_jdbc_connection(config, "local")
#     print(f"Connection = {conn}")
#     assert conn._closed == False
    
class TestCreateConnectionUrl(unittest.TestCase):
    def test_create_connection_url_with_all_properties(self):
        server = Server(name="testfull", schemas=[],dialect="mysql", driver="pymysql", user="root", password="password", host="localhost", port=3306, database="mydb")
        expected_url = "mysql+pymysql://root:password@localhost:3306/mydb"

        actual_url = utils.create_connection_url(server)

        self.assertEqual(actual_url, expected_url)

    def test_create_connection_url_without_optional_properties(self):
        server = Server(dialect="sqlite", database="mydb.db", name="testfull", schemas=[],)
        expected_url = "sqlite://mydb.db"

        actual_url = utils.create_connection_url(server)

        self.assertEqual(actual_url, expected_url)


def test_gapfill_partition():
    part_size = base.gap_fill_partition(0,4400000, 200, 1000)
    print(f"part_size: {part_size}")
    table = Table(name="Test1", full_name="test_gapfill_partition", schema="whatev")
    sql_partitions = base.generate_select_queries(min_id=0, max_id=4400000, 
                                                  partition_size=part_size, table=table) 
    for i in sql_partitions:
        print(f"sql_partitions: {i}")
        
if __name__ == "__main__":
    test_gapfill_partition()