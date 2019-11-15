## Project
#### Data Modeling with Postgres

### Brief Description

Sparkify is a music streaming startup app. It collects the data related to user activity , which takes place on their music app. Sparkify wants to analyze its song meta-data(song_data) in conjunction with the user-activity(log_data) to improve the user experience on their music app.

### Problem Statement
Sparkify stores the song and log data in the form of Json files. This makes it difficult for them to organize as well as analyze the data. 

### Objective
Sparkify want their data to be organised in such a way  that it can be queried and maintained easily.

### Solution

Organised the data in the form of a Postgres database with the tables following the star schema format. Star Schema is easy to understand, query, and maintain.

### Star Schema


![Tmage of Schema](https://github.com/anurag-code/Data-Engineering-Udacity/blob/master/PostgreSql/StarSchema.png)



Fact Table: songplays

Dimension Tables: users, artists, songs , and time 

The first column (Refer Schema image) in each of the dimension tables is the primary key. The songplays table is connected to all the dimension tables based on the foreign key constraints (Refer Schema image).

### ETL Process

#### Raw Data
Data from Sparkify is stored in the form of Json files. It is divided into two sets of folders:

1. songs meta data (folder name : song_data)

![Image of Songs meta data](https://github.com/anurag-code/Data-Engineering-Udacity/blob/master/PostgreSql/Songs_meta_data_json_files.png)

2. user activity data (folder name: log_data)

![user activity log data!](https://github.com/anurag-code/Data-Engineering-Udacity/blob/master/PostgreSql/User_activity_json_files.png)


#### Processed Dataframes
The data was extracted from the json files and converted into two dataframes
song dataframe and log dataframe.

1. Song dataframe

![song dataframe](https://github.com/anurag-code/Data-Engineering-Udacity/blob/master/PostgreSql/song_data.png)


2. Log dataframe

![log dataframe](https://github.com/anurag-code/Data-Engineering-Udacity/blob/master/PostgreSql/Log_data.png)


#### Python Files

**sql_queries.py** : This file has all the SQL based queries, which we will be using throughout the process. The variables in this file have been imported in all the files described below.

**create_tables.py** : This file connects to the database and creates all the tables with the placeholders.

**etl.py** : This file processes the data and inserts the records into the tables.

#### Order of Execution

1. open the terminal window.
2. Run create_table.py file first. Type 'python create_table.py' in terminal and hit run.
3. Run etl.py file. Type 'python etl.py' in terminal and hit run. 







