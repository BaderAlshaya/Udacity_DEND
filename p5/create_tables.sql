DROP TABLE IF EXISTS public.staging_events;
DROP TABLE IF EXISTS public.staging_songs;
DROP TABLE IF EXISTS public.songplays;
DROP TABLE IF EXISTS public.users;
DROP TABLE IF EXISTS public.songs;
DROP TABLE IF EXISTS public.artists;
DROP TABLE IF EXISTS public.time;

CREATE TABLE public.artists (
	artistid varchar(300) NOT NULL,
	name varchar(300),
	location varchar(300),
	lattitude numeric(20,0),
	longitude numeric(20,0)
);

CREATE TABLE public.songplays (
	playid varchar(32) NOT NULL,
	start_time timestamp NOT NULL,
	userid int4 NOT NULL,
	"level" varchar(300),
	songid varchar(300),
	artistid varchar(300),
	sessionid int4,
	location varchar(300),
	user_agent varchar(300),
	CONSTRAINT songplays_pkey PRIMARY KEY (playid)
);

CREATE TABLE public.songs (
	songid varchar(300) NOT NULL,
	title varchar(300),
	artistid varchar(300),
	"year" int4,
	duration numeric(20,0),
	CONSTRAINT songs_pkey PRIMARY KEY (songid)
);

CREATE TABLE public.staging_events (
	artist varchar(300),
	auth varchar(300),
	firstname varchar(300),
	gender varchar(300),
	iteminsession int4,
	lastname varchar(300),
	length numeric(20,0),
	"level" varchar(300),
	location varchar(300),
	"method" varchar(300),
	page varchar(300),
	registration numeric(20,0),
	sessionid int4,
	song varchar(300),
	status int4,
	ts int8,
	useragent varchar(300),
	userid int4
);

CREATE TABLE public.staging_songs (
	num_songs int4,
	artist_id varchar(300),
	artist_name varchar(300),
	artist_latitude numeric(20,0),
	artist_longitude numeric(20,0),
	artist_location varchar(300),
	song_id varchar(300),
	title varchar(300),
	duration numeric(20,0),
	"year" int4
);

CREATE TABLE public.users (
	userid int4 NOT NULL,
	first_name varchar(300),
	last_name varchar(300),
	gender varchar(300),
	"level" varchar(300),
	CONSTRAINT users_pkey PRIMARY KEY (userid)
);

CREATE TABLE time 
(
    start_time TIMESTAMP PRIMARY KEY,
    hour INTEGER,
    day INTEGER,
    week INTEGER,
    month INTEGER,
    year INTEGER,
    weekday INTEGER
)



