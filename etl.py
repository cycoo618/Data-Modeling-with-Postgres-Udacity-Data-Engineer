import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *
from datetime import datetime


def process_song_file(cur, filepath,check_later):
    '''Process song file from the filepath storing song lists
    
    Parameter:
        cur: cursor,
        filepath: filepath storing song lists,
        check_later: dummy argument
        
    '''
    
    # open song file
    df = pd.read_json(filepath,lines=True)

    # insert song record
    song_data = df[['song_id','title','artist_id','year','duration']].values.tolist()[0]
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = df[['artist_id', 'artist_name', 'artist_location', \
                      'artist_latitude', 'artist_longitude']].values.tolist()[0]
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath,check_later):
    '''Process log files
    
    Parameters:
        cur: cursor,
        filepath: filepath storing log files,
        check_later: data validation list,
        
    Returns:
        check_later: updated data validation list, which is storing the songs, artists and durations when the records are not in the Sparkify DB.
    
    '''
    
    # open log file
    df = pd.read_json(filepath,lines=True)

    # filter by NextSong action
    df = df[df['page'] == 'NextSong']

    # convert timestamp column to datetime

    t = df['ts'].apply(lambda x:datetime.fromtimestamp(x/1000.0))
    
    # insert time data records
    time_data = (t.apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S')),
                 t.dt.hour,
                 t.dt.day,
                 t.dt.week,
                 t.dt.month,
                 t.dt.year,
                 t.dt.weekday)
    column_labels = ('start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday')
    time_df = pd.DataFrame(dict(zip(column_labels,time_data)))

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
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None
            check_later.append([row.song, row.artist, row.length])

        # insert songplay record
        songplay_data = (datetime.fromtimestamp(row['ts']/1000.0).strftime("%Y-%m-%d %H:%M:%S")
                         , row['userId']
                         , row['level']
                         , songid
                         , artistid
                         , row['sessionId']
                         , row['location']
                         , row['userAgent'])
        cur.execute(songplay_table_insert, songplay_data)
    return check_later
        

def process_data(cur, conn, filepath, func):
    '''Iterate through the files in the specified filepath and process the data with specified pre-process functions.
    
    Parameters:
        cur: cursor, 
        conn: connection, 
        filepath: filepath to be specify, 
        func: function to be specify
        
    Returns:
        check_list: data validation result in csv file, which is storing the songs, artists and durations when the records are not in the Sparkify DB
        
    '''
    
    # get all files matching extension from directory
    all_files = []
    check_later=[]
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile,check_later)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))
    
    #save the unmatched records to a csv file for adding them to DB later
    if func == process_log_file:
        check_list = pd.DataFrame(data = check_later,columns=['song','artist','length'])
        check_list.to_csv('check_list.csv',index=False)
        print('{} logs do not have matched songs and artists in the Sparkify DB'.format(len(check_list)))
        print('They have been stored in this file check_list.csv so that you can add them to the DB later!')


def main():
    '''Main function runs the ETL pipeline'''
    
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()
    check_later=[]

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)


    conn.close()


if __name__ == "__main__":
    main()