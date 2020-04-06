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
    inserts it into the song and artist Postgres tables. Each file contains only
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
    
def create_temp_tables(cur):
    """ Creates temporary tables with no data for user data (tempusers),
    time data (temptime) and songplay data (tempsongplay)"""
    for query in create_temp_table_queries:
        cur.execute(query)
    
def bulk_insert_df_to_temptable(df, cur, temptablename):
    """ Writes the data frame df supplied in the arguments into a StringIO object,
    which is then bulk inserted into an already-created Postgres temporary table
    (temptablename) using psycopg2's copy_from function."""
    obj = StringIO()
    obj.write(df.to_csv(sep='\t', header=None, index=None))
    obj.seek(0)
    cur.copy_from(obj, temptablename, columns=df.columns)
    
def move_from_temp_to_main_tables(cur, conn):
    """ Moves data from the temporary tables created (temptime, tempusers,
    tempsongplays) into the main tables (time, users, songlplays). Handles
    conflicts (duplicates) with the ON CONFLICT DO... statement in Postgres."""
    # NOTE: The insert into statement for the users table first orders by
    # the timestamp in the tempusers table, thereby ensuring the latest values
    # of the fields for the given user_id are present.
    for query in insert_from_temp_to_main_queries:
        cur.execute(query)
    conn.commit()

def drop_temp_tables(cur):
    """ Drops temporary tables for user data (tempusers), time data (temptime)
    and songplay data (tempsongplay)"""
    for query in drop_temp_table_queries:
        cur.execute(query)
        
def process_log_file(cur, filepath):
    """ Reads the log file whose location is given in the filepath argument, bulk inserts into the users, time and songplays tables via temporary tables."""
    df = pd.read_json(filepath, lines=True)
    # filter by NextSong action
    df = df[df.page=='NextSong']
    df = df.rename(columns={'ts': 'start_time'})
    # Create a time df and create a pd.timestamp column from ts.
    time_df = df.loc[:, ['start_time']]
    time_df['tstamp'] = pd.to_datetime(time_df.start_time, unit='ms')
    
    column_labels = ['hour', 'day', 'week', 'month', 'year', 'weekday']
    time_df[column_labels] = time_df.loc[:, ['tstamp']].apply(get_timestamp_components, axis=1,
                                                          result_type='expand')
    time_df = time_df.drop(['tstamp'], axis=1)
    # Bulk insert into a temp table temptime 
    bulk_insert_df_to_temptable(time_df, cur, 'temptime')
        
    # load user table
    user_df = df.loc[:, ['start_time', 'userId', 'firstName', 'lastName', 'gender', 'level']]
    # Rename data frame columns to match Postgres table columns
    user_df= user_df.rename(columns={"userId":"user_id", "firstName":"first_name",
                       "lastName":"last_name"})
    # Handle duplicate users in the current file.
    user_df = user_df.drop_duplicates(subset='user_id', keep="last")
    # Bulk insert into to a temp table tempusers.
    # Duplicate users in other files will be handled by postgres's 'on conflict' statement
    # (update to latest value)
    bulk_insert_df_to_temptable(user_df, cur, 'tempusers')
    
    # insert songplay records
    for index, row in df.iterrows():
        # get songid and artistid from song and artist tables: lowercase the song and artist before searching
        cur.execute(song_select, (row.song.lower(), row.artist.lower(), row.length))
        results = cur.fetchone()
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record into a temporary songplay table as the songplay table has foreign keys from the empty users and time tables.
        songplay_data = (row.start_time, row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
        cur.execute(temp_songplay_table_insert, songplay_data)

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
    create_temp_tables(cur)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)
    move_from_temp_to_main_tables(cur, conn)
    print("Inserted data into all tables!")
    drop_temp_tables(cur)
    conn.close()

if __name__ == "__main__":
    main()