import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import TimestampType
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.functions import from_unixtime

#--------------------------------------------------------------------
config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']
#--------------------------------------------------------------------

def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark
#---------------------------------------------------------------------

def process_song_data(spark, input_data, output_data):
    """
        Description: This function loads song_data from S3 and extracts the songs and artist tables
        and then write back those tables in S3 in parquet format.
        
        Parameters:
            spark       : Spark Session
            input_data  : location of song_data json files 
            output_data : S3 bucket where extracted tables are written in parquet format.
    """
    # get filepath to song data file
    song_data =input_data + 'song_data/*/*/*/*.json'
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    # first create a view to run sql query and then write sql query
    df.createOrReplaceTempView("song")
    
    # query based on above view
    songs_table = spark.sql("SELECT DISTINCT song_id, title, artist_id, year, duration FROM song WHERE song_id IS NOT NULL ")
    
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy("year", "artist_id").parquet(output_data + 'songs/')


    # extract columns to create artists table
    artists_table = spark.sql("SELECT DISTINCT artist_id, artist_name AS name, artist_location AS location ,artist_latitude AS latitude, \
                             artist_longitude AS longitude FROM song WHERE artist_id IS NOT NULL")
    
    # write artists table to parquet files
    artists_table.write.parquet(output_data + 'artists/')

#------------------------------------------------------------------------------

def process_log_data(spark, input_data, output_data):
    """
        Description: This function loads log_data from S3 and extracts the songs and artist tablesafter processing
        and then write those generated tables to S3 in parquet format. Also output from previous function is used in by spark.read.json command
        
        Parameters:
            spark       : Spark Session
            input_data  : Location of log_data files
            output_data : S3 bucket where extracted tables are written in parquet format.
            
    """
    # get filepath to log data file
    log_data = input_data + 'log_data/*/*/*.json'

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')

    # extract columns for users table    
    users_table = df.select('userId','firstName', 'lastname', 'gender', 'level').dropDuplicates()\
                    .where(df.userId.isNotNull())
    
    # write users table to parquet files
    users_table.write.parquet(output+'users/')

    # create UDF for timestamp column from original timestamp column
    @udf(TimestampType())
    def conv_timestamp(ms):
        return datetime.fromtimestamp(ms/1000.0) 
    
    # Lets add one more column with correct usable time stamp format
    df = df.withColumn("start_time", conv_timestamp('ts'))
    
    # Create a dataframe which only has start_time
    log_time_data = df.select('start_time').dropDuplicates()\
                    .where(df.start_time.isNotNull())
    
    # extract columns to create time table
    time_table = log_time_data.withColumn('hour',hour('start_time'))\
                              .withColumn('day',dayofmonth('start_time'))\
                              .withColumn('week', weekofyear('start_time'))\
                              .withColumn('month', month('start_time'))\
                              .withColumn('year',year('start_time'))\
                              .withColumn("weekday", date_format("start_time", 'E'))
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy("year", "month").parquet(output_data + 'times/')
    
    # create a view for the log_data and we already have the view for song_data as song created at the start
    df.createOrReplaceTempView('log_data_filtered_timeformatted')

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = spark.sql("""SELECT monotonically_increasing_id() AS songplay_id,
                                  start_time,
                                  userId AS user_id,
                                  level,
                                  song_id,
                                  artist_id,
                                  sessionId AS session_id,
                                  location,
                                  userAgent AS user_agent
                                  FROM  log_data_filtered_timeformatted 
                                  JOIN song 
                                  ON artist = artist_name AND song = title """)

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy("year", "month").parquet(output_data + 'songplays/')

#------------------------------------------------------------------

def main():
    """
        1. Extract songs and events data from S3.
        2. Transforms that data into the facts and dimension tables.
        3. Write the generated tables back to S3 in parquet format.
    """
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://udacity-dend-dl/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
