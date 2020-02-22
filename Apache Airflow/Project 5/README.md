## Project: Data Pipelines with Airflow

### Brief Description:
A music streaming company, Sparkify, has decided that it is time to introduce more automation and monitoring to their data warehouse ETL pipelines and come to the conclusion that the best tool to achieve this is Apache Airflow.

### Objective:
Sparkify wants to create high grade data pipelines that are dynamic and built from reusable tasks, can be monitored, and allow easy backfills. They have also noted that the data quality plays a big part when analyses are executed on top the data warehouse and want to run tests against their datasets after the ETL steps have been executed to catch any discrepancies in the datasets.

The source data resides in S3 and needs to be processed in Sparkify's data warehouse in Amazon Redshift. The source datasets consist of JSON logs that tell about user activity in the application and JSON metadata about the songs the users listen to.


### Solution:
Build an ETL Airflow pipeline, which will do the following:

. Extracts the events and songs data from source S3.

. Create the songs and events related staging table in Redshift.

. Create facts and Dimensions table from staging tables in Redshift.

. Airflow has been used to create the full ETL workflow with all the connection credentials configured in airflow-connections.

Note : Airflow details are mentioned below.


### Project Datasets

Song Data Path --> s3://udacity-dend/song_data

Log Data Path --> s3://udacity-dend/log_data Log Data

### Project: Airflow Folder :  /home/workspace/airflow

The Airflow folder contains two main folders:
##### 1. dags : This folder contains two dags. 
##### 1.1 create_tables_dag.py : this is meant to create tables in the redshift before loading them with data.
##### 1.2 etl_dag.py: this is meant to run all the etl related and data quality related tasks.

In the DAG, add default parameters according to these guidelines

-The DAG does not have dependencies on past runs
-On failure, the task are retried 3 times
-Retries happen every 5 minutes
-Catchup is turned off
-Do not email on retry
-Building the Operators

##### 2. plugins : This folder contains two more folders; operators , and helpers. 

Four custom operators have been created in operators folder. These operators are meant to stage the data, transform the data, and run checks on data quality.  

##### 2.1 Stage Operator: stage_redshift.py

The stage operator is expected to be able to load any JSON and CSV formatted files from S3 to Amazon Redshift. The operator creates and runs a SQL COPY statement based on the parameters provided. The operator's parameters should specify where in S3 the file is loaded and what is the target table.

The parameters are used to distinguish between JSON and CSV file. Another important feature of this operator stage operator is that it contains a templated field that allows it to load timestamped files from S3 based on the execution time and run backfills.

##### 2.2 Fact and Dimension Operators : load_fact.py, load_dimension.py

These operators makes use of the SQL Helper class (refer helper folder) to run data transformations. Most of the logic is within the SQL transformations and the operator takes as input a SQL statement and target database on which to run the query against. Dimension loads are done with the truncate-insert pattern whereas the target table is emptied before the load. Fact tables are usually so massive that they only allow append type functionality.

##### 2.3 Data Quality Operator

The final operator created is the data quality operator, which is used to run checks on the data itself. The operator's main functionality is to receive one or more SQL based test cases along with the expected results and execute the tests. For each the test, the test result and expected result are checked and if there is no match, the operator raise an exception and the task should retry and fail eventually.

For example: one test could be a SQL statement that checks if certain column contains NULL values by counting all the rows that have NULL in the column. We do not want to have any NULLs so expected result would be 0 and the test would compare the SQL statement's outcome to the expected result.

##### Final Instructions - Inside udacity vpc:

When you are in the workspace, after completing the code, you can start by using the command : /opt/airflow/start.sh

Once you done, it would automatically start the airflow web UI. 