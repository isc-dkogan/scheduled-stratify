import sqlite3
from dotenv import load_dotenv

load_dotenv()

# TODO - Needs to be from env var
con = sqlite3.connect("/home/data/insights.db")  

cur = con.cursor()

drop_table_stats = "DROP TABLE IF EXISTS Table_Stats"
drop_histogram = "DROP TABLE IF EXISTS  Histogram"
drop_job = "DROP TABLE IF EXISTS  Job"
drop_table_schema = "DROP TABLE IF EXISTS Table_Schema"
drop_table_topcounts = "DROP TABLE IF EXISTS TopCounts"

# TABLE_SCHEMA,TABLE_NAME,field_count,FULL_NAME,row_count
create_table_stats_sql = """CREATE TABLE Table_Stats (job_id integer, database_name varchar(50), schema_name varchar(50), table_name varchar(50),
                                                      field_count integer, full_name varchar(50), row_count integer)"""

# TABLE_SCHEMA,TABLE_NAME,COLUMN_NAME,DATA_TYPE,CHARACTER_MAXIMUM_LENGTH,UNIQUE_COLUMN,
# IS_NULLABLE,PRIMARY_KEY,IS_IDENTITY,Index_Name,Ordinal_Position,Non_Unique,FULL_NAME
create_table_schema = """CREATE TABLE Table_schema (job_id integer, db_name varchar(50), schema_name varchar(50),table_name varchar(50),column_name varchar(50),data_type varchar(50)
                        ,character_maximum_length integer ,UNIQUE_COLUMN varchar(50), is_nullable varchar(50),primary_key varchar(50),is_identity varchar(50),
                        index_name varchar(50), ordinal_position integer, non_unique varchar(50), min double, max double, avg double, stddev double,
                        distinctcount integer)""" 

create_table_histogram_sql = """CREATE TABLE  Histogram (db_instance varchar(50), db_name varchar(50), table_name varchar(50), field_name varchar(50),
                                job_id integer, bucket_number integer, minval double, maxval double, count integer);"""

create_table_jobs_sql = """CREATE TABLE Job (JobID INTEGER PRIMARY KEY, Timestamp TIMESTAMP, Config varchar(10000), Completed INTEGER)"""

create_table_topcounts = """CREATE TABLE  TopCounts (db_instance varchar(50), db_name varchar(50), table_name varchar(50), field_name varchar(50),
                                job_id integer, rank integer, value varchar(300), count integer)"""
 
# DROP
cur.execute(drop_job)
cur.execute(drop_histogram)
cur.execute(drop_table_stats)
cur.execute(drop_table_schema)
cur.execute(drop_table_topcounts)

# CREATE
cur.execute(create_table_histogram_sql)
cur.execute(create_table_jobs_sql)
cur.execute(create_table_stats_sql)
cur.execute(create_table_schema)
cur.execute(create_table_topcounts)

def create_tables(connection):
    cur = connection.cursor()
    cur.execute(create_table_histogram_sql)
    cur.execute(create_table_jobs_sql)
    cur.execute(create_table_stats_sql)
    cur.execute(create_table_schema)