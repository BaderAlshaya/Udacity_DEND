
# DROP TABLES
song_plays_table_drop = "DROP TABLE IF EXISTS song_plays"
users_table_drop = "DROP TABLE IF EXISTS users"
songs_table_drop = "DROP TABLE IF EXISTS songs"
artists_table_drop = "DROP TABLE IF EXISTS artists"
times_table_drop = "DROP TABLE IF EXISTS times"


# CREATE TABLES
song_plays_table_create = ("""
CREATE TABLE IF NOT EXISTS song_plays (
	songplay_id SERIAL PRIMARY KEY,
	user_id INT REFERENCES users (user_id),
	song_id VARCHAR REFERENCES songs (song_id),
	artist_id VARCHAR REFERENCES artists (artist_id),
	start_time TIMESTAMP REFERENCES times (start_time),
	session_id INT NOT NULL,
	level VARCHAR,
	location VARCHAR,
	user_agent TEXT
)""")

users_table_create = ("""
CREATE TABLE IF NOT EXISTS users (
	user_id INT PRIMARY KEY,
	first_name VARCHAR,
	last_name VARCHAR,
	gender CHAR(1),
	level VARCHAR
)""")

songs_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (
	song_id VARCHAR PRIMARY KEY,
	artist_id VARCHAR REFERENCES artists (artist_id),
	title VARCHAR,
	year INT CHECK (year >= 0),
	duration FLOAT
)""")

artists_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (
	artist_id VARCHAR PRIMARY KEY,
	name VARCHAR,
	location VARCHAR,
	latitude DECIMAL(9,6),
	longitude DECIMAL(9,6)
)""")

times_table_create = ("""
CREATE TABLE IF NOT EXISTS times (
	start_time TIMESTAMP PRIMARY KEY,
	hour INT NOT NULL CHECK (hour >= 0),
	day INT NOT NULL CHECK (day >= 0),
	week INT NOT NULL CHECK (week >= 0),
	month INT NOT NULL CHECK (month >= 0),
	year INT NOT NULL CHECK (year >= 0),
	weekday VARCHAR NOT NULL
)""")


# INSERT RECORDS
song_plays_table_insert = ("""
INSERT INTO song_plays 
VALUES (DEFAULT, %s, %s, %s, %s, %s, %s, %s, %s)
""")

users_table_insert = ("""
INSERT INTO users (user_id, first_name, last_name, gender, level) 
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (user_id) DO UPDATE SET
level = EXCLUDED.level
""")

songs_table_insert = ("""
INSERT INTO songs (song_id, artist_id, title, year, duration)
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (song_id) DO NOTHING
""")

artists_table_insert = ("""
INSERT INTO artists (artist_id, name, location, latitude, longitude)
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (artist_id) DO NOTHING
""")

times_table_insert = ("""
INSERT INTO times VALUES (%s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (start_time) DO NOTHING
""")


# FIND SONGS
songs_select = ("""
SELECT song_id, artists.artist_id
FROM songs JOIN artists ON songs.artist_id = artists.artist_id
WHERE songs.title = %s
AND artists.name = %s
AND songs.duration = %s
""")


# QUERY LISTS
create_table_queries = [
    times_table_create,
    users_table_create, 
    artists_table_create, 
    songs_table_create, 
    song_plays_table_create
]

drop_table_queries = [
    times_table_drop,
    users_table_drop,
    artists_table_drop,
    songs_table_drop,
    song_plays_table_drop
]
