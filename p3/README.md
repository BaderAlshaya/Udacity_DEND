# Project 3: Data Warehouse

&nbsp;

## Introduction
A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app. 

Therefore, they'd like us to build an ETL pipeline that extracts their data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for their analytics team to continue finding insights in what songs their users are listening to.

We'll be able to test our database and ETL pipeline by running queries given to us by the analytics team from Sparkify and compare our results with their expected results.

&nbsp;

## Project Description
**Requirements:**
Using the song and event datasets, we will create a star schema optimized for queries on song play analysis. This includes the following tables.

- Fact Table
1. songplays - records in event data associated with song plays i.e. records with page NextSong 
(songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)

- Dimension Tables
2. users - users in the app
(user_id, first_name, last_name, gender, level)
3. songs - songs in music database
(song_id, title, artist_id, year, duration)
4. artists - artists in music database
(artist_id, name, location, lattitude, longitude)
5. time - timestamps of records in songplays broken down into specific units
(start_time, hour, day, week, month, year, weekday)

&nbsp;

**Files:**
- (Python File) **`create_tables.py`**: Creates the star schema in Redshift.
- (Python File) **`sql_queries.py`**: Defines the SQL queries for the star schema and ETL pipeline.
- (Python File) **`etl.py`**: Implements the ETL pipeline to extract, load, and process data into tables on Redshift.

&nbsp;
