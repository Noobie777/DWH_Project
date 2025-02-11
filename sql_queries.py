import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

ARN = config.get("IAM_ROLE","ARN").strip("'")

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS events_data"
staging_songs_table_drop = "DROP TABLE IF EXISTS songs_data"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time_table"

# CREATE TABLES

staging_events_table_create= (""" 
Create table events_data (
                              artist varchar,
                              auth varchar ,
                              firstName varchar,
                              gender char (1) ,
                              itemInSession int ,
                              lastName varchar,
                              length double precision,
                              level char (4) ,
                              location varchar ,
                              method char (3) ,
                              page varchar ,
                              registration varchar ,
                              sessionId int ,
                              song varchar,
                              status int ,
                              ts timestamp ,
                              userAgent varchar ,
                              userId int 
)
""")

staging_songs_table_create = (""" 
Create table songs_data (
                              num_songs int,
                              artist_id varchar ,
                              artist_latitude double precision,
                              artist_longitude double precision,
                              artist_location varchar,
                              artist_name varchar,
                              song_id varchar PRIMARY KEY,
                              title varchar ,
                              duration double precision ,
                              year int 
)
""")

songplay_table_create = (""" 
Create table songplays (
                         songplay_id int IDENTITY(0,1) PRIMARY KEY,
                         start_time timestamp REFERENCES public.time_table(start_time) ,
                         user_id int REFERENCES users(user_id),
                         level char (4) ,
                         song_id varchar REFERENCES songs(song_id),
                         artist_id varchar REFERENCES artists(artist_id),
                         session_id varchar ,
                         location varchar ,
                         user_agent varchar 
)
""")

user_table_create = (""" 
Create table users (
                     user_id int PRIMARY KEY,
                     first_name varchar,
                     last_name varchar,
                     gender char (1) ,
                     level char (4) 
)
""")

song_table_create = (""" 
Create table songs (
                     song_id varchar PRIMARY KEY,
                     title varchar ,
                     artist_id varchar ,
                     year int ,
                     duration double precision 
)
""")

artist_table_create = (""" 
Create table artists (
                       artist_id varchar PRIMARY KEY,
                       name varchar ,
                       location varchar ,
                       latitude double precision,
                       longitude double precision
)
""")

time_table_create = (""" 
Create table time_table (
                     start_time timestamp PRIMARY KEY,
                     hour int , 
                     day int , 
                     week int , 
                     month int , 
                     year int , 
                     weekday boolean 
)
""")

# STAGING TABLES

staging_events_copy = ("""
                       copy events_data from {}
                       credentials 'aws_iam_role={}'
                       JSON {}
                       compupdate off
                       region 'us-west-2'
                       TIMEFORMAT 'epochmillisecs'
                       
""").format(config.get("S3","LOG_DATA"),
            ARN,
            config.get("S3","LOG_JSONPATH"))

staging_songs_copy = ("""
                      copy songs_data from {}
                      credentials 'aws_iam_role={}'
                      JSON 'auto'
                      compupdate off
                      region 'us-west-2'

""").format(config.get("S3","SONG_DATA"), ARN)

# FINAL TABLES

songplay_table_insert = ("""
Insert into songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
                         SELECT DISTINCT ed.ts AS start_time,
                            ed.userId AS user_id,
                            ed.level AS level,
                            sd.song_id AS song_id,
                            sd.artist_id AS artist_id,
                            ed.sessionId AS session_id,
                            ed.location AS location,
                            ed.userAgent AS user_agent
                         FROM events_data ed
                         JOIN songs_data sd
                         ON ed.artist = sd.artist_name
                         WHERE user_id IS NOT NULL AND song_id IS NOT NULL AND artist_id IS NOT NULL

""")

user_table_insert = ("""
Insert into users (user_id, first_name, last_name, gender, level)
                     SELECT DISTINCT ed.userId AS user_id,
                     ed.firstName AS first_name,
                     ed.lastName AS last_name,
                     ed.gender AS gender,
                     ed.level as level
                     FROM events_data ed
                     WHERE ed.userId IS NOT NULL
""")

song_table_insert = ("""
Insert into songs (song_id, title, artist_id, year, duration)
                     SELECT DISTINCT sd.song_id AS song_id,
                     sd.title AS title,
                     sd.artist_id AS artist_id,
                     sd.year AS year,
                     sd.duration AS duration
                     FROM songs_data sd
                     WHERE sd.song_id IS NOT NULL
""")

artist_table_insert = ("""
Insert into artists (artist_id, name, location, latitude, longitude)
                       SELECT DISTINCT sd.artist_id AS artist_id,
                       sd.artist_name AS name,
                       sd.artist_location AS location,
                       sd.artist_latitude AS latitude,
                       sd.artist_longitude AS longitude
                       FROM songs_data sd
                       WHERE sd.artist_id IS NOT NULL
""")


time_table_insert = ("""
Insert into time_table (start_time, hour, day, week, month, year, weekday)
                     SELECT DISTINCT ed.ts::TIMESTAMP AS start_time,
                     EXTRACT(hour FROM ed.ts::TIMESTAMP) AS hour,
                     EXTRACT(day FROM ed.ts::TIMESTAMP) AS day,
                     EXTRACT(week FROM ed.ts::TIMESTAMP) AS week,
                     EXTRACT(month FROM ed.ts::TIMESTAMP) AS month,
                     EXTRACT(year FROM ed.ts::TIMESTAMP) AS year,
                     CASE WHEN EXTRACT(ISODOW FROM ed.ts) IN (6, 7) THEN false ELSE true END AS weekday
                     FROM events_data ed
                     WHERE start_time IS NOT NULL
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
