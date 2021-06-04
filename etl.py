import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    """
    -Read the song file
    -Extract and insert songs data into the songs table
    -Extract and insert artists data into the artists table
    """
    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record
    song_data = df.values.tolist()
    song_data = [song_data[0][7]] + [song_data[0][8]] + [song_data[0][0]] + [song_data[0][9]] + [song_data[0][5]] 
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = df.values.tolist()
    artist_data = [song_data[0][7]] + [song_data[0][8]] + [song_data[0][0]] + [song_data[0][9]] + [song_data[0][5]] 
    cur.execute(artist_table_insert, artist_data)

def bulk_insert(cur, df, create_tmp_table, tmp_table, bulk_insert):
    """
    - Bulk inserts dataframe to destination table.
    - Creates temporary csv
    - Opens csv and copys into the database table
    - Deletes temporary csv
    """
    tmp_csv = "./tmp.csv"
    # create temporary csv to use bulk insert to database
    df.to_csv(tmp_csv, header=False, index=False, sep="\t")
    f = open(tmp_csv, "r")
    # create temp table in order to not violate unique constraint
    cur.execute(create_tmp_table)
    cur.copy_from(f, tmp_table, sep="\t")
    cur.execute(bulk_insert)
    # Remove temporary csv
    os.remove(tmp_csv)


def process_log_file(cur, filepath):
    """
    - Read the log file
    - Filter page column of log data by NextSong to get lists of valid played songs
    - Convert timestamp column of log data to datetime
    - Extract the timestamp, hour, day, week of year, month, year, and weekday from the timestamp column of log data and 
      insert time data records to time table
    - Extract and insert users data into the users table
    - Extract and populate the SongPlay table from the log data including song_id and artist_id columns generated from 
      the songs and artists tables
    """
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df['page'] == 'NextSong']
    
    # convert timestamp column to datetime
    t = df['ts'] = pd.to_datetime(df['ts'], unit='ms')
    
    # insert time data records
    time_data = [df['ts'], t.dt.hour, t.dt.day, t.dt.week, t.dt.month, t.dt.year, t.dt.weekday]
    column_labels = ['start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday']
    time_df = pd.DataFrame(
                              dict(zip(
                                        column_labels, time_data
                                     ))
                          )

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[["userId", "firstName", "lastName", "gender", "level"]].drop_duplicates(subset=['userId'])
    
    # insert user records
    bulk_insert(
        cur=cur,
        df=user_df,
        create_tmp_table=create_tmp_users_table,
        tmp_table="tmp_users",
        bulk_insert=users_table_bulk_insert,
    )
    
    # create temporary csv to use bulk insert to database
    songplay_df = df[
        [
            "ts",
            "userId",
            "level",
            "sessionId",
            "location",
            "userAgent",
            "song",
            "artist",
            "length",
        ]
    ]
    
    bulk_insert(
        cur=cur,
        df=songplay_df,
        create_tmp_table=create_tmp_songplays_table,
        tmp_table="tmp_songplays",
        bulk_insert=songplays_table_bulk_insert,
    )

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
        songplay_data = (row.ts, row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """
    -Get lists of all JSON files in data/song_data and data/log_data directories
    -Get total number of files found in each directories
    -Run through each file and process them by calling function process_song_file() and process_log_file()

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