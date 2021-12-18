import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')
S3_LOG_DATA = config.get('S3','LOG_DATA')
S3_LOG_JSONPATH = config.get('S3','LOG_JSONPATH')
S3_SONG_DATA = config.get('S3','SONG_DATA')
IAM_ROLE_ARN = config.get('IAM_ROLE','ARN')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events (
artist varchar,
auth varchar,
firstName varchar,
gender varchar,
itemInSession integer,
lastName varchar,
length varchar,
level varchar,
location varchar,
method varchar,
page varchar,
registration bigint,
sessionId integer,
song varchar,
status integer,
ts bigint,
userAgent varchar,
userId integer
)
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs (
num_songs integer,
artist_id varchar,
artist_latitude varchar null,
artist_longitude varchar null,
artist_location varchar,
artist_name varchar,
song_id varchar,
title varchar,
duration decimal,
year integer
)
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays (
songplay_id integer IDENTITY(0,1) NOT NULL PRIMARY KEY SORTKEY,
start_time timestamp NOT NULL REFERENCES time(start_time), 
user_id varchar NOT NULL REFERENCES users(user_id), 
level varchar, 
song_id varchar REFERENCES songs(song_id), 
artist_id varchar DISTKEY REFERENCES artists(artist_id), 
session_id varchar, 
location varchar, 
user_agent varchar
)
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (
user_id varchar PRIMARY KEY DISTKEY SORTKEY, 
first_name varchar, 
last_name varchar, 
gender varchar, 
level varchar
)
DISTSTYLE KEY
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (
song_id varchar NOT NULL PRIMARY KEY SORTKEY,
title varchar NOT NULL,
artist_id varchar NOT NULL REFERENCES artists(artist_id),
year integer,
duration decimal
)
DISTSTYLE ALL
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (
artist_id varchar NOT NULL PRIMARY KEY SORTKEY, 
artist_name varchar NOT NULL, 
artist_location varchar NULL, 
artist_latitude varchar NULL, 
artist_longitude varchar NULL
)
DISTSTYLE ALL
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (
start_time timestamp NOT NULL PRIMARY KEY DISTKEY SORTKEY, 
hour integer NOT NULL, 
day integer NOT NULL, 
week integer NOT NULL, 
month integer NOT NULL, 
year integer NOT NULL, 
weekday varchar NOT NULL
)
DISTSTYLE KEY

""")

# STAGING TABLES

staging_events_copy = ("""
COPY staging_events
FROM {}
CREDENTIALS 'aws_iam_role={}'
FORMAT AS JSON {}
BLANKSASNULL
EMPTYASNULL
COMPUPDATE OFF region 'us-west-2'
""").format(S3_LOG_DATA, IAM_ROLE_ARN, S3_LOG_JSONPATH)

staging_songs_copy = ("""
COPY staging_songs
FROM {}
CREDENTIALS 'aws_iam_role={}'
FORMAT AS JSON 'auto'
BLANKSASNULL
EMPTYASNULL
COMPUPDATE OFF region 'us-west-2'
""").format(S3_SONG_DATA, IAM_ROLE_ARN)

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT DISTINCT 
TIMESTAMP 'epoch' + (se.ts / 1000) * INTERVAL '1 second'
, se.userId
, se.level
, ss.song_id
, ss.artist_id
, se.sessionId
, se.location
, se.userAgent
FROM staging_events se
LEFT JOIN staging_songs ss ON se.song = ss.title AND se.artist = ss.artist_name
WHERE se.page = 'NextSong';
""")
#ON CONFLICT DO NOTHING;

user_table_insert = ("""
INSERT INTO users (user_id, first_name, last_name, gender, level)
SELECT DISTINCT
se.userId
, se.firstName
, se.lastName
, se.gender
, se.level
FROM staging_events se
WHERE se.page = 'NextSong';
""")
#ON CONFLICT (user_id) DO UPDATE SET level = excluded.level;

song_table_insert = ("""
INSERT INTO songs (song_id, title, artist_id, year, duration) 
SELECT DISTINCT
ss.song_id
, ss.title
, ss.artist_id
, ss.year
, ss.duration
FROM staging_songs ss;
""")
#ON CONFLICT DO NOTHING;

artist_table_insert = ("""
INSERT INTO artists (artist_id, artist_name, artist_location, artist_latitude, artist_longitude)
SELECT DISTINCT
ss.artist_id
, ss.artist_name
, ss.artist_location
, ss.artist_latitude
, ss.artist_longitude
FROM staging_songs ss;
""")
#ON CONFLICT DO NOTHING;


time_table_insert = ("""
INSERT INTO time (start_time, hour, day, week, month, year, weekday)
SELECT DISTINCT 
TIMESTAMP 'epoch' + (se.ts / 1000) * INTERVAL '1 second' AS start_time
, EXTRACT(HOUR FROM start_time) AS hour
, EXTRACT(DAY FROM start_time) AS day
, EXTRACT(WEEKS FROM start_time) AS week
, EXTRACT(MONTH FROM start_time) AS month
, EXTRACT(YEAR FROM start_time) AS year
, to_char(start_time, 'Day') AS weekday
FROM staging_events se
WHERE se.page = 'NextSong';
""")
#ON CONFLICT DO NOTHING;


# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, artist_table_create, song_table_create, time_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
#create_table_queries = [user_table_create, artist_table_create, song_table_create, time_table_create, songplay_table_create]
#drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [user_table_insert, song_table_insert, artist_table_insert, time_table_insert, songplay_table_insert]
