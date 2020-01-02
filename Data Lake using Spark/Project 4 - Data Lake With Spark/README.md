## Project: Data Lake

### Brief Description and Objective
A music streaming startup, Sparkify, has grown their user base and song database exponentially, thus their data warehouse doesnt seem to be an effective solution. Therefore, Sparkify wants to move their data from their data warehouse to a data lake. 

### Solution:
Build an ETL pipeline, which will do the following:

. Extracts the data from source S3.

. Process the data using Spark deployed on EMR cluster on AWS.

. Load the tables generated from the original data source back to S3 in parquet format.

. These tables will help us in creating a data mart, with which we will be able to find actionable insights.

**NOTE:** The code given here is meant to run Spark locally and connecting it with S3 to extract and store the data; however, the same code 
with very slight modification can run on the EMR cluster on AWS.

### ETL pipeline
-- Load credentials in the configuration (dl.cfg) file.

-- Read data from S3

- Song data: s3://udacity-dend/song_data
- Log data: s3://udacity-dend/log_data

-- Process data using PySpark to create five different tables. Each table includes the right columns and data types. 
Duplicates are addressed where appropriate. The new tables generated are as follows:
- Fact Table: songplays

- Dimension Tables: users, artists, songs , and time

-- Load the generated tables back to S3 in parquet format.

-- Each of the five tables are written to parquet files in a separate analytics directory on S3. Each table has its own folder within the directory. Songs table files are partitioned by year and then artist. Time table files are partitioned by year and month. Songplays table files are partitioned by year and month. 

### Project Files
-- To run this project in local mode, create a file **dl.cfg** in the root of this project with the following data:

   YOUR_AWS_ACCESS_KEY

   YOUR_AWS_SECRET_KEY

-- **Test.ipynb** is a jupyter notebook, which can be used to test all the functions, which we will write in the final etl script (etl.py).
   This file is also very useful because I have used both SQL functions and data frame functions of Pyspark to create process 
   data create the new tables. **'data_local'** is a folder which has the sample data which is used in test.ipynb
   

-- Create an S3 Bucket (**s3://udacity-dend-dl/**) named 'udacity-dend-dl' where output tables will be stored.

-- Finally, run the following command in terminal:

   ***python etl.py***

-- **NOTE**: To run on an Jupyter Notebook powered by an EMR cluster, import the notebook found in this project.


