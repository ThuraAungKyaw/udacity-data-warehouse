import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

ARN = config['IAM_ROLE']['ARN']
LOG_DATA = config['S3']['LOG_DATA']
SONG_DATA = config['S3']['SONG_DATA']
LOG_JSONPATH = config['S3']['LOG_JSONPATH']

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create= ("""CREATE TABLE staging_events (
                                artist VARCHAR,
                                auth VARCHAR,
                                firstName VARCHAR,
                                gender VARCHAR(1),
                                itemInSession INTEGER,
                                lastName VARCHAR,
                                length NUMERIC,
                                level VARCHAR(4),
                                location VARCHAR,
                                method VARCHAR(6),
                                page VARCHAR,
                                registration NUMERIC,
                                sessionId INTEGER,
                                song VARCHAR,
                                status SMALLINT,
                                ts BIGINT,
                                userAgent VARCHAR,
                                userId INTEGER
)
""")

staging_songs_table_create = ("""CREATE TABLE staging_songs (
                                num_songs INTEGER, 
                                artist_id VARCHAR, 
                                artist_latitude NUMERIC, 
                                artist_longitude NUMERIC, 
                                artist_location VARCHAR, 
                                artist_name VARCHAR, 
                                song_id VARCHAR, 
                                title VARCHAR, 
                                duration NUMERIC, 
                                year SMALLINT);
                            """)

songplay_table_create = ("""CREATE TABLE songplays (
     songplay_id BIGINT IDENTITY(0,1) NOT NULL PRIMARY KEY,
     start_time TIMESTAMP NOT NULL sortKey,
     user_id VARCHAR NOT NULL distKey,
     level VARCHAR(4) NOT NULL, 
     song_id VARCHAR,
     artist_id VARCHAR,
     session_id INTEGER NOT NULL, 
     location VARCHAR, 
     user_agent VARCHAR NOT NULL
     );
""")

user_table_create = ("""CREATE TABLE users (
                        user_id VARCHAR NOT NULL PRIMARY KEY sortKey distKey, 
                        first_name VARCHAR NOT NULL, 
                        last_name VARCHAR NOT NULL, 
                        gender VARCHAR(1) NOT NULL, 
                        level VARCHAR(4) NOT NULL);
""")

song_table_create = ("""
      CREATE TABLE songs (
          song_id VARCHAR NOT NULL PRIMARY KEY sortKey, 
          title VARCHAR NOT NULL, 
          artist_id VARCHAR NOT NULL,
          year SMALLINT NOT NULL, 
          duration NUMERIC NOT NULL
          ) distStyle all;
""")

artist_table_create = ("""
    CREATE TABLE artists (
         artist_id VARCHAR NOT NULL PRIMARY KEY sortKey, 
         name VARCHAR NOT NULL, 
         location VARCHAR, 
         latitude NUMERIC, 
         longitude NUMERIC) distStyle all;
""")

time_table_create = ("""
    CREATE TABLE time (
        start_time TIMESTAMP NOT NULL PRIMARY KEY sortKey, 
        hour SMALLINT NOT NULL, 
        day SMALLINT NOT NULL, 
        week SMALLINT NOT NULL, 
        month SMALLINT NOT NULL, 
        year SMALLINT NOT NULL, 
        weekday SMALLINT NOT NULL);
""")

# STAGING TABLES

staging_events_copy = ("""COPY staging_events
                            FROM {} 
                            iam_role {}
                            region 'us-west-2'
                            json {};
""").format(LOG_DATA, ARN, LOG_JSONPATH)

staging_songs_copy = ("""COPY staging_songs
                            FROM {} 
                            iam_role {}
                            region 'us-west-2' 
                            json 'auto';
""").format(SONG_DATA, ARN)

# FINAL TABLES

songplay_table_insert = ("""INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT DISTINCT TIMESTAMP 'epoch' + ste.ts/1000 * INTERVAL '1 second' AS start_time, 
    ste.userId as user_id,
    ste.level,
    sts.song_id,
    sts.artist_id,
    ste.sessionId as session_id,
    ste.location,
    ste.userAgent as user_agent
    FROM staging_events AS ste JOIN staging_songs AS sts
    ON ste.artist = sts.artist_name
    AND ste.length = sts.duration
    AND ste.song = sts.title
    WHERE ste.page = 'NextSong';
""")

user_table_insert = ("""INSERT INTO users (user_id, first_name, last_name, gender, level)
    SELECT DISTINCT userId as user_id,
                    firstName as first_name, 
                    lastName as last_name, 
                    gender, 
                    level
    FROM staging_events WHERE page = 'NextSong' 
    AND userId NOT IN (SELECT DISTINCT user_id FROM users);
""")

song_table_insert = ("""INSERT INTO songs (song_id, title, artist_id, year, duration)
    SELECT DISTINCT song_id, 
                    title, 
                    artist_id, 
                    year, 
                    duration
    FROM staging_songs
    WHERE song_id NOT IN (SELECT DISTINCT song_id from songs);
""")

artist_table_insert = ("""INSERT INTO artists (artist_id, name, location, latitude, longitude)
    SELECT DISTINCT artist_id, 
                    artist_name as name, 
                    artist_location as location, 
                    artist_latitude as latitude, 
                    artist_longitude as longitude
    FROM staging_songs
    WHERE artist_id NOT IN (SELECT DISTINCT artist_id from artists)
""")

time_table_insert = ("""INSERT INTO time (start_time, hour, day, week, month, year, weekday)
     SELECT DISTINCT TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second' AS start_time, 
                     extract(hour from start_time),
                     extract(day from start_time),
                     extract(week from start_time),
                     extract(month from start_time),
                     extract(year from start_time),
                     extract(weekday from start_time)
    FROM staging_events
    WHERE ts NOT IN (SELECT DISTINCT start_time from time)
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [user_table_insert, song_table_insert, artist_table_insert, time_table_insert, songplay_table_insert]
