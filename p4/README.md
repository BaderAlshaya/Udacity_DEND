# Project 4: Data Lake

&nbsp;

## Introduction
A music streaming startup, Sparkify, has grown their user base and song database even more and want to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

Therefore, they'd like us to build an ETL pipeline that extracts their data from S3, processes them using Spark, and loads the data back into S3 as a set of dimensional tables. This will allow their analytics team to continue finding insights in what songs their users are listening to.

We'll be able to test our database and ETL pipeline by running queries given to us by the analytics team from Sparkify and compare our results with their expected results. 

&nbsp;

## Project Description
**Requirements:**
Using the song and event datasets, we will create a star schema optimized for queries on song play analysis. This includes the following tables.

- Fact Table
1. **songplays - records in event data associated with song plays i.e. records with page NextSong**

(songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)

- Dimension Tables
2. **users - users in the app**

(user_id, first_name, last_name, gender, level)

3. **songs - songs in music database**

(song_id, title, artist_id, year, duration)

4. **artists - artists in music database**

(artist_id, name, location, lattitude, longitude)

5. **time - timestamps of records in songplays broken down into specific units**

(start_time, hour, day, week, month, year, weekday)**

&nbsp;

**Files:**
- (Python File) **`etl.py`**: reads data from S3, processes that data using Spark, and writes them back to S3.
- (Config File) **`dl.cfg`**: [not pushed to GitHub repo] contains your AWS credentials

&nbsp;
