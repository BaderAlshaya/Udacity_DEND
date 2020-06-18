import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


"""
Processes the JSON files for song metadata using ETL Pipeline

Steps:
1. Reads songs metadata from a local JSON file
2. Inserts the metadata into artists and songs tables

Keyword arguments:
cur -- the database connected cursor
filepath -- the song JSON files path
"""
def process_song_file(cur, filepath):
    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert artist record
    artist_data = df[['artist_id',
                      'artist_name',
                      'artist_location',
                      'artist_latitude',
                      'artist_longitude']].values[0]
    cur.execute(artists_table_insert, artist_data)

    # insert song record
    song_data = df[['song_id', 
                    'artist_id', 
                    'title', 
                    'year', 
                    'duration']].values[0]
    cur.execute(songs_table_insert, song_data)


"""
Processes the JSON files for log metadata using ETL Pipeline

Steps:
1. Reads logs metadata from a local JSON file
2. Filters logs by NextSong page action
3. Inserts the metadata into times, users and song_plays tables

Keyword arguments:
cur -- the database connected cursor
filepath -- the log JSON files path
"""
def process_log_file(cur, filepath):
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df['page'] == 'NextSong']

    # convert timestamp column to datetime
    df['ts'] = pd.to_datetime(df['ts'], unit='ms')
    
    # insert time data records
    time_data = [df['ts'], 
                 df['ts'].dt.hour,
                 df['ts'].dt.day,
                 df['ts'].dt.weekofyear,
                 df['ts'].dt.month,
                 df['ts'].dt.year,
                 df['ts'].dt.weekday]
    
    column_labels = ['ts',
                     'hour',
                     'day',
                     'week of year',
                     'month',
                     'year',
                     'weekday']    
    
    time_df = pd.DataFrame.from_dict(dict(zip(column_labels, time_data)))

    for i, row in time_df.iterrows():
        cur.execute(times_table_insert, list(row))

    # load user table
    user_df = df[['userId',
                  'firstName',
                  'lastName',
                  'gender',
                  'level']]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(users_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(songs_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (row.userId,
                         songid,
                         artistid,
                         pd.to_datetime(row.ts, unit='ms'),
                         row.sessionId,
                         row.level,
                         row.location,
                         row.userAgent)
        cur.execute(song_plays_table_insert, songplay_data)


"""
Collects the song and log JSON files and execute their processing functions

Steps:
1. Locates the json files path and append them in one list
2. Iterates over each JSON file and call its processing function
3. Print the number and the processing result of each JSON file

Keyword arguments:
cur -- the database connected cursor
con -- the connection to database
filepath -- the JSON file path
func -- the processing function
"""
def process_data(cur, conn, filepath, func):
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