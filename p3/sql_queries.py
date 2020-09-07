import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')


# DROP TABLES
staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS song_plays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS times"


# CREATE TABLES
staging_events_table_create= ("""
    CREATE TABLE IF NOT EXISTS staging_events (
        artist VARCHAR(100),
        auth VARCHAR(100),
        first_name VARCHAR(50),
        gender VARCHAR(1),
        item_in_session INTEGER, 
        last_name VARCHAR(50),
        legnth DECIMAL(15, 5),
        level VARCHAR(15),
        location VARCHAR(200),
        method VARCHAR(15),
        page VARCHAR(50),
        registration DECIMAL(15, 1),
        session_id INTEGER,
        song VARCHAR(300),
        status INTEGER,
        ts BIGINT,
        user_agent VARCHAR(100),
        user_id VARCHAR(15)
    );
""")


staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs (
        num_songs INTEGER,
        artist_id VARCHAR(15), 
        artist_latitude DECIMAL(15, 5),
        artist_longitude DECIMAL(15, 5),
        artist_location VARCHAR(200),
        artist_name VARCHAR(100),
        song_id VARCHAR(15),
        title VARCHAR(200),
        duration DECIMAL(15, 5),
        year INTEGER
    );
""")


songplay_table_create = ("""
    CREATE TABLE song_plays (
        song_play_id INTEGER IDENTITY(0,1) PRIMARY KEY,
        start_time TIMESTAMP NOT NULL, 
        user_id VARCHAR(15),
        level VARCHAR(15),
        song_id VARCHAR(200) NOT NULL,
        artist_id VARCHAR(15) NOT NULL,
        session_id INTEGER,
        location VARCHAR(200),
        user_agent VARCHAR(100)
    );
""")


user_table_create = ("""
    CREATE TABLE users (
        user_id VARCHAR(15) PRIMARY KEY,
        first_name VARCHAR(50),
        last_name VARCHAR(50),
        gender VARCHAR(1),
        level VARCHAR(15)
    );
""")


song_table_create = ("""
    CREATE TABLE songs (
        song_id VARCHAR(15) PRIMARY KEY,
        title VARCHAR(200) NOT NULL,
        artist_id VARCHAR(15),
        year INTEGER,
        duration DECIMAL(15, 5) NOT NULL
    );
""")


artist_table_create = ("""
    CREATE TABLE artists (
        artist_id VARCHAR(15) PRIMARY KEY,
        name VARCHAR(200) NOT NULL,
        location VARCHAR(200),
        lattitude DECIMAL(15, 5),
        longitude DECIMAL(15, 5)
    );
""")


time_table_create = ("""
    CREATE TABLE times (
        start_time TIMESTAMP PRIMARY KEY,
        hour INTEGER,
        day INTEGER,
        week INTEGER,
        month INTEGER,
        year INTEGER,
        weekday INTEGER
    );
""")


# STAGING TABLES
staging_events_copy = ("""
    COPY staging_events 
    FROM {} 
    iam_role {}
    FORMAT AS JSON {};""").format(config.get('S3', 'LOG_DATA'),
                                  config.get('IAM_ROLE', 'ARN'), 
                                  config.get('S3', 'LOG_JSONPATH'))

staging_songs_copy = ("""
    COPY staging_songs 
    FROM {} 
    iam_role {}
    FORMAT AS JSON 'auto';""").format(config.get('S3', 'SONG_DATA'), 
                                      config.get('IAM_ROLE', 'ARN'))


# FINAL TABLES
songplay_table_insert = ("""
    INSERT INTO song_plays (
        start_time,
        user_id,
        level,
        song_id,
        artist_id,
        session_id,
        location,
        user_agent
    ) 
    SELECT timestamp 'epoch' + (e.ts / 1000) * interval '1 second',
        e.user_id,
        e.level,
        s.song_id,
        s.artist_id,
        e.session_id,
        e.location,
        e.user_agent
    FROM staging_events e
    INNER JOIN staging_songs s on e.song = s.title 
                              AND e.artist = s.artist_name 
                              AND e.legnth = s.duration
    WHERE e.page = 'NextSong'
""")


user_table_insert = ("""
    INSERT INTO users (
        user_id,
        first_name,
        last_name,
        gender,
        level
    )
    SELECT
        e.user_id,
        e.first_name,
        e.last_name,
        e.gender,
        e.level
    FROM staging_events e
    WHERE NOT EXISTS (
        SELECT 1 
        FROM staging_events e2 
        WHERE e.user_id = e2.user_id 
        AND e.ts < e2.ts
    )
""")


song_table_insert = ("""
    INSERT INTO songs (
        song_id,
        title,
        artist_id,
        year,
        duration
    )
    SELECT
        s.song_id,
        s.title,
        s.artist_id,
        CASE 
            WHEN s.year != 0 
            THEN s.year 
            ELSE NULL END as year,
            s.duration
    FROM staging_songs s
""")


artist_table_insert = ("""
    INSERT INTO artists (
        artist_id,
        name,
        location,
        lattitude,
        longitude
    )
    SELECT 
        artist_id, 
        artist_name, 
        artist_location, 
        artist_latitude, 
        artist_longitude
    FROM (
        SELECT
            s.artist_id,
            s.artist_name,
            s.artist_location,
            s.artist_latitude,
            s.artist_longitude,
            row_number() 
            OVER (partition by s.artist_id ORDER BY s.year desc)
      FROM staging_songs s
    )
    WHERE row_number = 1
""")


time_table_insert = ("""    
    INSERT INTO times (
        start_time,
        hour,
        day,
        week,
        month,
        year,
        weekday
    )
    SELECT 
        start_time,
        EXTRACT(hour FROM times) as hour,
        EXTRACT(day FROM times) as day,
        EXTRACT(week FROM times) as week,
        EXTRACT(month FROM times) as month,
        EXTRACT(year FROM times) as year,
        EXTRACT(weekday FROM times) as weekday
    FROM (
        SELECT 
            DISSTINCT p.start_time
    FROM song_plays p
    )
""")


# QUERY LISTS
create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
