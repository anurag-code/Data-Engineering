"""
etl.py defines necessary functions, which helps us in extracting the raw data from the json files,
process the extracted raw data and then load it into the data warehouse.

"""

import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *
import numpy as np


"""
In our functions below we are trying to insert the dataframe into  postgres DB but it will fail if we do not  convert
the existing datatype in the the dataframe 'numpy.int64' to the pyscop2 compatible.
Therefore, from psycopg2.extensions, register_adapter and AsIs have been imported.
"""
from psycopg2.extensions import register_adapter, AsIs
psycopg2.extensions.register_adapter(np.int64, psycopg2._psycopg.AsIs)



def process_song_file(cur, filepath):
    """
    Processes the raw data of song_data file.
    
    This function processes the raw song_data file: extracts the data , process it and then loads it into the two
    tables (songs and artists).
    
    Parameters:
        cur: a database cursor.
        filepath: a string filepath.
    """
    # open song file
    df = pd.read_json(filepath,lines=True)

    # insert song record
    song_data = df.loc[0,['song_id', 'title', 'artist_id', 'year', 'duration']].values.tolist()
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = df.loc[0,['artist_id' , 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']].values.tolist()
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """
    Processes the raw data of log_data file.
    
    This function processes the raw log_data file: extracts the data , process it and then loads it into the three
    tables (time, user and songplay).
    
    Parameters:
        cur: a database cursor.
        filepath: a string filepath.
    """
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df.loc[df.page == 'NextSong', :]

    # convert timestamp column to datetime
    t = pd.to_datetime(df.ts, unit='ms')
    
    # insert time data records
    time_data = {'start_time':list(df.ts), 'hour': list(t.dt.hour), 'day': (t.dt.day), 'week': list(t.dt.week), 'month': list(t.dt.month), 'year': list(t.dt.year),\
             'weekday':list(t.dt.weekday)}
    #column_labels = 
    time_df = pd.DataFrame(time_data)

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df.loc[:, ['userId','firstName','lastName', 'gender', 'level']]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (row.ts, row.userId,row.level,songid,artistid,row.sessionId,row.location,row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """
    Function to get the file paths of raw data and processing of raw data using 
    aforementioned functions (process_song_file, process_log_file).
    Parameters:
        cur: database cursor.
        conn: database connection.
        filepath: string filepath to song_data or log_data
        func: function names of the functions defined above (process_song_file, process_log_file)
    """
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    """
    Creates object conn to connect to the database.
    Opens a cursor object (cur) to perform database operations.
    Processes the data using afore-mentioned function process_data() .
    """
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()