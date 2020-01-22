from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.dummy_operator import DummyOperator

default_args = {
    'owner': 'udacity',
    'start_date': datetime(2019, 1, 12),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_retry': False,
    'depends_on_past': False,
    'catchup': False
}


dag = DAG('create_table_dag',
          default_args=default_args,
          description='Create Tables in Redshift with Airflow',
          schedule_interval='0 * * * *',
          catchup=False
        )



#------------Create Staging Tables in Redshift ------------

table_artists = ("""
CREATE TABLE IF NOT EXISTS artists
(
     artist_id             VARCHAR   PRIMARY KEY,
     name                  VARCHAR,
     location              VARCHAR,
     latitude              FLOAT,
     longitude             FLOAT)
diststyle all;
""")

table_songplays = ("""
CREATE TABLE IF NOT EXISTS songplays
(
     songplay_id          INTEGER IDENTITY(0,1)  PRIMARY KEY   sortkey,
     start_time           TIMESTAMP,
     user_id              INTEGER   NOT NULL,
     level                VARCHAR,
     song_id              VARCHAR   NOT NULL,
     artist_id            VARCHAR   NOT NULL,
     session_id           INTEGER,
     location             VARCHAR,
     user_agent           VARCHAR
);
""")

table_songs= ("""CREATE TABLE IF NOT EXISTS songs (
	songid varchar(256) NOT NULL,
	title varchar(256),
	artistid varchar(256),
	year int4,
	duration numeric(18,0),
	CONSTRAINT songs_pkey PRIMARY KEY (songid)
);""")

table_staging_events = ("""
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
""")

table_staging_songs = ("""
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
""")
table_time = ("""CREATE TABLE IF NOT EXISTS time (
	start_time timestamp NOT NULL,
	hour int4,
	day int4,
	week int4,
	month varchar(256),
	year int4,
	weekday varchar(256),
	CONSTRAINT time_pkey PRIMARY KEY (start_time)
) ;""")

table_users = ("""
CREATE TABLE IF NOT EXISTS users
(
     user_id              INTEGER    PRIMARY KEY,
     first_name           VARCHAR,
     last_name            VARCHAR,
     gender               VARCHAR,
     level                VARCHAR)
diststyle all;
""")


start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

create_table_staging_events = PostgresOperator(
                        task_id= "Create_staging_events_table",
                        dag = dag,
                        postgres_conn_id = "redshift",
                        sql = table_staging_events
                        )

create_table_staging_songs = PostgresOperator(
                        task_id= "Create_staging_songs_table",
                        dag = dag,
                        postgres_conn_id = "redshift",
                        sql = table_staging_songs
                        )

create_table_songs = PostgresOperator(
                        task_id= "Create_songs_table",
                        dag = dag,
                        postgres_conn_id = "redshift",
                        sql = table_songs
                        )

create_table_artists = PostgresOperator(
                        task_id= "Create_artists_table",
                        dag = dag,
                        postgres_conn_id = "redshift",
                        sql =table_artists
                        )

create_table_time = PostgresOperator(
                        task_id= "Create_time_table",
                        dag = dag,
                        postgres_conn_id = "redshift",
                        sql = table_time
                        )

create_table_users = PostgresOperator(
                        task_id= "Create_users_table",
                        dag = dag,
                        postgres_conn_id = "redshift",
                        sql = table_users
                        )

create_table_songplays = PostgresOperator(
                        task_id= "Create_songplays_table",
                        dag = dag,
                        postgres_conn_id = "redshift",
                        sql = table_songplays
                        )

# -------------------------------------------------------------------

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

#-------- DAG dependencies-----------------------------------------



start_operator >> [create_table_staging_events , create_table_staging_songs] >> create_table_songplays

create_table_songplays >> [create_table_songs,  create_table_artists, create_table_time , create_table_users] >> end_operator
