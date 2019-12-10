## Project
#### Data Warehousing - AWS Redshift

### Brief Description

Sparkify is a music streaming startup app. It collects the data related to user activity , which takes place on their music app. Sparkify wants to analyze its song meta-data(song_data) in conjunction with the user-activity(log_data) to improve the user experience on their music app.

### Problem Statement
Sparkify stores the song and log data in the form of Json files in S3 buckets. This makes it difficult for them to organize as well as analyze the data. 

### Objective
Sparkify want their data to be organised in such a way  that it can be queried faster and maintained easily.

### Solution

- Organised the data in the form of a AWS Redshift database. 
- Used the existing song and events Json data in S3 and created the Staging Tables with S3 datasets.
- Stored that Staging Table in Redshift Cluster.
- Used that Staging Table to inject data into the Target Tables of a database having a Star Schema.
- Delete the Staging Table after the updation of the Target Tables.
- Finally, the star schema tables are easy to query and maintain.


### Star Schema


![Tmage of Schema](https://github.com/anurag-code/Data-Engineering-Udacity/blob/master/PostgreSql/StarSchema.png)



Fact Table: songplays

Dimension Tables: users, artists, songs , and time 

The first column (Refer Schema image) in each of the dimension tables is the primary key. The songplays table is connected to all the dimension tables based on the foreign key constraints (Refer Schema image).

### ETL Process

#### Raw Data
Data from Sparkify is stored in the form of Json files in AWS S3 bucket 'udacity-dend'. It is further divided into two sets of folders as follows:

1. songs data (Song data: s3://udacity-dend/song_data)

![Image of Songs meta data](https://github.com/anurag-code/Data-Engineering-Udacity/blob/master/PostgreSql/Songs_meta_data_json_files.png)

2. log data (Log data: s3://udacity-dend/log_data)

![user activity log data!](https://github.com/anurag-code/Data-Engineering-Udacity/blob/master/PostgreSql/User_activity_json_files.png)


#### Data Staging
The data was extracted from the json files and fed into two staging tables
staging_events and staging_songs.

#### Target Tables
Data from Staging Tables was used to create Target Tables as a Star Schema.


### Python Files

**DataStaging_Redshift_Test.ipynb** : This file shows step by step process to create the Staging Tables in Redshift through Jupyter Notebook.

**dwh.cfg** : This file has all the cofiguraion details of the Redshift-Cluster, IAM roles, S3 , and AWS account credentials.

**sql_queries.py** : This file has all the SQL based queries, which we will be using throughout the process. The sql queries have been stored in the form of variables and those variables have been imported in all the files described below.

**create_tables.py** : This file connects to the database and creates all the tables.

**etl.py** : This file processes the data and inserts the records into the tables.

#### Order of Execution

1. open the terminal window.
2. Save all the config details in dwh.cfg.
2. Run create_table.py file first. Type 'python create_table.py' in terminal and hit run.
3. Run etl.py file. Type 'python etl.py' in terminal and hit run.