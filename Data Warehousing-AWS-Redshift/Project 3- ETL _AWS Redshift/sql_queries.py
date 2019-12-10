import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# GLOBAL VARIABLES
LOG_DATA   = config.get("S3","LOG_DATA")
LOG_PATH   = config.get("S3", "LOG_JSONPATH")
SONG_DATA  = config.get("S3", "SONG_DATA")
IAM_ROLE   = config.get("IAM_ROLE","ARN")

# DROP TABLES

staging_events_table_drop   = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop    = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop         = "DROP TABLE IF EXISTS fact_songplay"
user_table_drop             = "DROP TABLE IF EXISTS dim_user"
song_table_drop             = "DROP TABLE IF EXISTS dim_song"
artist_table_drop           = "DROP TABLE IF EXISTS dim_artist"
time_table_drop             = "DROP TABLE IF EXISTS dim_time"

# CREATE TABLES

staging_events_table_create= ("""
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


staging_songs_table_create = ("""
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

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS fact_songplay
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

user_table_create = ("""
CREATE TABLE IF NOT EXISTS dim_user
(
     user_id              INTEGER    PRIMARY KEY,
     first_name           VARCHAR,
     last_name            VARCHAR,
     gender               VARCHAR,
     level                VARCHAR)
diststyle all;
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS dim_song
(
     song_id               VARCHAR   PRIMARY KEY,
     title                 VARCHAR,
     artist_id             VARCHAR   NOT NULL,
     year                  INTEGER,
     duration              FLOAT)
diststyle all;
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS dim_artist
(
     artist_id             VARCHAR   PRIMARY KEY,
     name                  VARCHAR,
     location              VARCHAR,
     latitude              FLOAT,
     longitude             FLOAT)
diststyle all;
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS dim_time
(
     start_time           TIMESTAMP PRIMARY KEY   sortkey,
     hour                 INTEGER   NOT NULL,
     day                  INTEGER   NOT NULL,
     week                 INTEGER   NOT NULL,
     month                INTEGER   NOT NULL,
     year                 INTEGER   NOT NULL,
     weekday              INTEGER   NOT NULL)
diststyle all;
""")




# STAGING TABLES

staging_events_copy = ("""
                       COPY staging_events FROM {}
                       CREDENTIALS 'aws_iam_role={}'
                       COMPUPDATE OFF region 'us-west-2'
                       TIMEFORMAT as 'epochmillisecs'
                       TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL
                       FORMAT AS JSON {};
                    """).format(LOG_DATA, IAM_ROLE, LOG_PATH)



staging_songs_copy = ("""
                        COPY staging_songs FROM {}
                        CREDENTIALS 'aws_iam_role={}'
                        COMPUPDATE OFF region 'us-west-2'
                        FORMAT AS JSON 'auto' 
                        TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL;
                    """).format(SONG_DATA, IAM_ROLE)




# FINAL TABLES

songplay_table_insert = ("""INSERT INTO fact_songplay(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
                            SELECT  se.ts as start_time,
                            se.userId, 
                            se.level, 
                            ss.song_id, 
                            ss.artist_id, 
                            se.sessionId, 
                            se.location, 
                            se.userAgent
                            FROM staging_events se
                            JOIN staging_songs ss
                            ON se.song =ss.title AND se.artist = ss.artist_name
                            WHERE page = 'NextSong'
""")

user_table_insert = ("""INSERT INTO dim_user(user_id, first_name, last_name, gender, level)
                        SELECT DISTINCT  userId, 
                        firstName, 
                        lastName, 
                        gender, 
                        level
                        FROM staging_events
                        WHERE page = 'NextSong'
""")

song_table_insert = ("""INSERT INTO dim_song(song_id, title, artist_id, year, duration)
                        SELECT DISTINCT song_id, 
                        title, 
                        artist_id, 
                        year, 
                        duration
                        FROM staging_songs                       
""")

artist_table_insert = ("""INSERT INTO dim_artist(artist_id, name, location, latitude, longitude)
                          SELECT DISTINCT artist_id, 
                          artist_name, 
                          artist_location, 
                          artist_latitude, 
                          artist_longitude 
                          FROM staging_songs
""")

time_table_insert = ("""INSERT INTO dim_time(start_time, hour, day, week, month, year, weekDay)
                        SELECT DISTINCT start_time, 
                        extract(hour from start_time), 
                        extract(day from start_time),
                        extract(week from start_time), 
                        extract(month from start_time),
                        extract(year from start_time), 
                        extract(dayofweek from start_time)
                        FROM songplay
""")


# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
