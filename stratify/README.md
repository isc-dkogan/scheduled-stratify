# Insights

## Local installtion and testing
    1. Create a virtual env and activate
    2. From code root directory, cd python
    3. pip install -r requirements.txt
    4. pip install -e . 
    5. Run 1st example
       1. python insights/examples/simple_random_add.py 
    6.  Example 2
       1. sqlite3 insights/tests/data/test.db < insights/tests/data/position_create_insert.sql
       2. sqlite3 insights/tests/data/test.db < insights/tests/data/security_master_create_insert.sql
       3. In the histogram_config.conf file change the field database to match exact location of above test.db file in the servers object with name test
       4. python insights/framework/sqltodf.py ../configs/histogram_config.conf 

# Steps to get deployed to SDS
1. Be able to use build_all_steps.sh, build_helm.sh and deploy_to_cluster.sh to deploy to the DEV EKS cluster

    __Example__
    $ ./build_all_steps.sh insights 0.1.1
    $ ./build_helm.sh /Users/psulin/projects/sds-insights insights 0.1.1 0.1.1
    $ ./deploy_to_cluster.sh insights -- insights /Users/psulin/projects/insights/helm/

2. Update the repo SDS uses for deployment, https://isc-patrick.github.io/sds-insights/
   1. You will provide your local version of this repo in the first arg to build_helm.sh and the last arg of deploy_to_cluster.sh
3. Deploy to EKS DEV SDS via the Control Plane
4. Make sure this is well documented in this file

5. Use this database to store strat results: jdbc:IRIS://iris-svc.iris:1972/SDS. This is an iris instance in the EKS cluster
















Nov 13-17th
 - Add requires and creates fields in comments for all functions. Requires stipulates fields and objects needed from context and creates states what fields/objects are created in context.
   - This could also be done in a more formal way by creating Context subclasses and passing these as arguments and letting pydantic throw errors. In order to insure that the return object is created in context, the returned object would also have to be type checked.
 - Move over required code from fs-athena
   - fastapi.py 
   - build scripts 
 - Add more error handling and exception logging
   - Review fs-athena to see what is being handled now 
   - Resolve issue with ray throwing pickle error when using the logger on a remote task/actor
 - Merge all of David's code
 - Formalize the concept of the reduce function  
 - Update this README to include referneces to docs and brief summary of examples
 - Refactor for single code path for all single column commputations
   - Add a custom code folder for dynamic importing of new functions that are not in compute.py
   - Completely config driven
 - Add ability to get aggregates using the framework instead of SQL aggregate functions
 - Add a Step decorator that handles basic logging and error handling
 - Do a deployment to SDS
 - Add loadenv for handling passwords in configs and default config
   - Standardize naming convention


























1. Configuration driven services
   1. Open API 3.0 compliant implementation
      1. Schema which defines required fields and data types
   2. Basic Configurations, with implicit logic, that can later be defined explicitly
2. Dynamic, multi-node scalable services
3. In-memory object store 
4. Connectors - ETL
5. Consistent application metrics


# High level features
1. Configuration based

## Getting started


### TODO
0. Implement database abstraction for using IRIS(RDBMS), SQLlite with file for integration tests,
and SQLlite in memory for unit testing
1. Modify config to be pydantic compatible - all keys are property names not values
2. All variables accessed through Context
3. ?Refactor calls to ray to not use decorators but instead just use function wrappers, which will allow using another concurrency framework as longs as get/wait semantics are same or can be emulated.
4. Refactor map-reduce using workflow execution and execute_job
5. Introduce job re-entry
6. Determine if workflow_execution and compile will be same code path with compilation being determined by flags: dry_run, generate skeleotn code(this one would create a python file with all methods and signatures in the workflow json)


# Serialization and data formats
__Solve for this example__
IrisMetadata -> Instances -> Databases -> Schemas -> Tables -> Columns
    - Each child is a list of ItemType
    - Lookup is always done on the name field, but could be done on other fields though not garaunteed to be unique
    - Uniqueness of names should be checked
    - Each object has a container base class that wraps the items in a list
        class Instances(BaseList)
            items : List(Instance)
    - Adding a child item will always check to see if a parent exists and add if not
        - This means that iterating through the unique columns will create entire structure
    - class BaseList
        def to_records(): -> list of lists each containing comma seperated fields
        def to_json(recursive: boolean=False): -> string
        def to_dic(recursive: boolean=False): -> dict
        def to_df(): -> Dataframe
        def from_*(): Reverse of all above

    
### Things we need to decide and document
1. Naming standards
    1. DB
        1. Tables, columns, views
2. Framework
   1.  Clarify exactly when and why attributes are used vs dictionary in Context and Config objects

### General Requirements
0. Scalable containerized application tier
1. Python and Java
2. Monitoring, logging in real time

### Requirements
0. Dockererized Python and Java base application layer
1. Ability to do columnar and row level calculations with minimal table scans, ideally just 2. One can be done for some use cases where initial metadata like min, max, distinct counts, etc... are not needed.
    0. Columnar
        0. Histograms
        1. Top X Distinct with Count(*)
    1. Row
        0. Validations
2. Capable of dynamic scaling based on data size and job
3. Caching: TTL and checkpoints
4. Deployable to Cloud(AWS to start)
5. REST endpoints
    0. Endpoint for invoking each job, async retruns job id
    1. Status and retrieval endpoint that takes job id
6. Configuration driven. The job can be kicked off with a single configuration.
7. 


Concurrency API
Create an API for concurrency that allows usage of either Ray or Threads or Gevent.
RAY api used: 
    get, wait, remote
Gevent
    wait, spawn

API method mapping