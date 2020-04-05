import os
import glob
from io import StringIO
import psycopg2
import pandas as pd
from sql_queries import *

def replace_with_nones(df, item_to_replace, column_list):
    """ Replaces item_to_replace with Nones in the columns specified in 'column_list'
    in the data frame, so that they show up as NULLS in Postgres
    Note: all columns will be converted to 'object' dtype when np.nans are replaced"""
    # Strange behaviour observed when replacing nans and '' with None in all columns
    # This needs to be done separately for different columns
    df[column_list] = df[column_list].replace({item_to_replace: None})
    #mod_df = mod_df.replace({'': None})
    return df

def get_timestamp_components(t):
    """ Takes t, a row from a Series of timestamps as input, extracts different time-related elements
    and returns them """
    # for timestamp, the returned format is HHMMSSffffff without colons.t.dt.strftime('%H%M%S%f')[0],
    return t.dt.hour[0], t.dt.day[0], t.dt.weekofyear[0], t.dt.month[0], t.dt.year[0], t.dt.weekday[0]

def process_song_file(cur, filepath):
    """ Reads the current song data file, loads it into a data frame, cleans it and
    bulk inserts it into the song and artist Postgres tables. Each file contains only
    one line. """
    # open song file
    df = pd.read_json(filepath, lines=True)
    # Data analysis revealed that there are np.nans in artist latitude andl longitude,
    # '' in artist location and 0 in year (of song)
    df = replace_with_nones(df, pd.np.nan, ['artist_latitude', 'artist_longitude'])
    df = replace_with_nones(df, '', ['artist_location'])
    # Years have 0s that need to be replaced with Nones
    df = replace_with_nones(df, 0, ['year'])
    
    # insert song record
    song_df = df[['song_id', 'title', 'artist_id', 'year', 'duration']]
    song_data = song_df.values[0].tolist()
    try:
        cur.execute(song_table_insert, song_data)
    except psycopg2.Error as e: 
        # psycopg2.IntegrityError if song_id is duplicate. Doesn't happen.
        print('Error encountered while inserting song data')
        print(e)
    
    # insert artist record
    artist_df = df[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']]
    artist_data = artist_df.values[0].tolist()
    try:
        cur.execute(artist_table_insert, artist_data)
    except psycopg2.Error as e: 
        print('Error encountered while inserting artist data')
        print(e)

def bulk_insert(df, cur, table_name):
    """ Takes a data frame, cursor and postgres table name as arguments,
    and does a bulk insert into the table using StringIO as a staging file"""
    obj = StringIO()
    obj.write(df.to_csv(sep='\t', header=None, index=None))
    obj.seek(0)
    # Use copy_from command  to insert the content of an entire log file into
    # the resp. postgres table.
    # Syntax: cur.copy_from(f, 'test', columns=('num', 'data')) 
    # https://www.psycopg.org/docs/cursor.html#cursor.copy_from
    cur.copy_from(obj, table_name, columns=tuple(df.columns))
        
def process_log_file(cur, filepath):
    df = pd.read_json(filepath, lines=True)
    # filter by NextSong action
    df = df[df.page=='NextSong']
    # Create a time df and create a pd.timestamp column from ts.
    time_df = df.loc[:, ['ts']]
    time_df['tstamp'] = pd.to_datetime(time_df.ts, unit='ms')
    time_df = time_df.rename(columns={'ts': 'start_time'})
    column_labels = ['hour', 'day', 'week', 'month', 'year', 'weekday']
    time_df[column_labels] = time_df.loc[:, ['tstamp']].apply(get_timestamp_components, axis=1,
                                                          result_type='expand')
    time_df = time_df.drop(['tstamp'], axis=1)
    # Drop duplicates if they exist. These records in time_df aren't tied to a 
    # particular user, so that should be perfectly fine.
    time_df = time_df.drop_duplicates(subset='start_time', keep="last")
    # insert time data records
    bulk_insert(time_df, cur, 'time')
    
    # load user table
    user_df = df.loc[:, ['userId', 'firstName', 'lastName', 'gender', 'level']]
    # Rename data frame columns to match Postgres table columns
    user_df= user_df.rename(columns={"userId":"user_id", "firstName":"first_name",
                       "lastName":"last_name"})
    # Drop duplicates records for the same user in the current log file.
    # Duplicate users in other log files will be handled by postgres's 'on conflict'
    # statement (update to latest value)
    user_df = user_df.drop_duplicates(subset='user_id', keep="last")
    # insert user records
    bulk_insert(user_df, cur, 'users')
    
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