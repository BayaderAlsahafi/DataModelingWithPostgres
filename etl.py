"""ETL for the Sparkify Database

This script performs the ETL (extract, trsnaform and load) processes on the Sparkify database.

This script requires you to run the create_tables.py script first, and to have the sql_queries.py file in your project folder.

This script requires the following libraries to be installed within the Python
environment you are running this script in:
    * os
    * glob
    * psycopg2
    * pandas
    * json
    * datetime
    * numpy
"""

import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *
import json
from datetime import datetime
import numpy as np


def process_song_file(cur, filepath):
    """ 
    A function to process the song file data and insert it to the database.
    
    Parameters: 
    cur (cursor): Psycopg2 cursor to execute PostgreSQL command in a database session.
    filepath (str): Path to the song file.
    """
    
    # open song file
    df = pd.read_json(filepath, lines = True)
    
    # load and insert song record
    song_data = list(df[['song_id', 'title', 'artist_id', 'year', 'duration']].values[0])
    cur.execute(song_table_insert, song_data)
    
    # load and insert artist record
    artist_data = list(df[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']].values[0])
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """ 
    A function to process the song file data and insert it to the database.
    
    Parameters: 
    cur (cursor): Psycopg2 cursor to execute PostgreSQL command in a database session.
    filepath (str): Path to the log file.  
    """

    # open log file
    df = pd.read_json(filepath, lines = True)

    # filter by NextSong action
    df = df[df['page'] == 'NextSong']

    # convert timestamp to datetime
    t = df['ts'] // 1000      #convert ms to sec by integer division
    t = t.apply(lambda x: datetime.fromtimestamp(x))
    
    # load time data records
    time_data = [(round(x.timestamp()), x.hour, x.day, x.week , x.month, x.year, x.weekday()) for x in t]
    column_labels = ("timestamp","hour", "day", "week", "month", "year", "weekday")
    time_df = pd.DataFrame(time_data, columns = column_labels)

    
    # insert time data records
    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchall()
        (songid, artistid) = results[0] if results else (None, None)

        # insert songplay record
        songplay_data = (row.userId, songid, artistid, row.ts, row.level, row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)

def process_data(cur, conn, filepath, func):
    """ 
    A function to extract files of data, apply the given function on it and commiting the changes.
    
    Parameters: 
    cur (cursor): Psycopg2 cursor to execute PostgreSQL command in a database session.
    conn (connection): Handles the connection to a PostgreSQL database instance.
    filepath (str): Path to the file to be processed.
    func (function): Function to be applied on data.
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
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()