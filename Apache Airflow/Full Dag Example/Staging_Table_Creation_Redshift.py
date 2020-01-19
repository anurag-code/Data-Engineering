import logging
import datetime

from airflow import DAG
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator

staging_events_table_create= """
CREATE TABLE IF NOT EXISTS staging_events
                        (
                        artist          VARCHAR,
                        auth            VARCHAR, 
                        firstName       VARCHAR,
                        gender          VARCHAR,   
                        itemInSession   INTEGER,
                        lastName        VARCHAR,
                        length          FLOAT,
                        level           VARCHAR, 
                        location        VARCHAR,
                        method          VARCHAR,
                        page            VARCHAR,
                        registration    BIGINT,
                        sessionId       INTEGER,
                        song            VARCHAR,
                        status          INTEGER,
                        ts              TIMESTAMP,
                        userAgent       VARCHAR,
                        userId          INTEGER
                        );
                        """

staging_songs_table_create = """
CREATE TABLE IF NOT EXISTS staging_songs
                                (
                                   song_id            VARCHAR,
                                   num_songs          INTEGER,
                                   title              VARCHAR,
                                   artist_name        VARCHAR,
                                   artist_latitude    FLOAT,
                                   year               INTEGER,
                                   duration           FLOAT,
                                   artist_id          VARCHAR,
                                   artist_longitude   FLOAT,
                                   artist_location    VARCHAR
                                );
                                """


COPY_SQL = """
               COPY staging_events
               FROM 's3://udacity-dend/log_data'
               ACCESS_KEY_ID '{}'
               SECRET_ACCESS_KEY '{}'
               COMPUPDATE OFF region 'us-west-2'
               TIMEFORMAT as 'epochmillisecs'
               TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL
               FORMAT AS JSON 's3://udacity-dend/log_json_path.json'
           """


COPY_SQL_SONGS = """
                COPY staging_songs
                FROM 's3://udacity-dend/song_data'
                ACCESS_KEY_ID '{}'
                SECRET_ACCESS_KEY '{}'
                COMPUPDATE OFF region 'us-west-2'
                FORMAT AS JSON 'auto' 
                TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL
               """
    


def load_data_to_redshift(*args, **kwargs):
    aws_hook = AwsHook("aws_credentials")
    credentials = aws_hook.get_credentials()
    redshift_hook = PostgresHook("redshift")
    redshift_hook.run(COPY_SQL.format(credentials.access_key, credentials.secret_key))
 
def load_data_to_redshift_songs(*args, **kwargs):
    aws_hook = AwsHook("aws_credentials")
    credentials = aws_hook.get_credentials()
    redshift_hook = PostgresHook("redshift")
    redshift_hook.run(COPY_SQL_SONGS.format(credentials.access_key, credentials.secret_key))

dag = DAG(
    'test_anu_17.01.2020',
    start_date=datetime.datetime.now()
)

create_table = PostgresOperator(
    task_id="create_table_",
    dag=dag,
    postgres_conn_id="redshift",
    sql=staging_events_table_create
 )

create_table_songs = PostgresOperator(
    task_id="create_table_songs_",
    dag=dag,
    postgres_conn_id="redshift",
    sql=staging_songs_table_create
 )



copy_task = PythonOperator(
    task_id='load_from_s3_to_redshift_ap',
    dag=dag,
    python_callable=load_data_to_redshift
)


copy_task_song = PythonOperator(
    task_id='load_from_s3_to_redshift_songs',
    dag=dag,
    python_callable=load_data_to_redshift_songs
)

create_table >> copy_task
create_table_songs>>copy_task_song