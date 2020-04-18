import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "drop table if exists stg_events;"
staging_songs_table_drop = "drop table if exists stg_songs;"
songplay_table_drop = "drop table if exists fact_songplay;"
user_table_drop = "drop table if exists dim_user;"
song_table_drop = "drop table if exists dim_song;"
artist_table_drop = "drop table if exists dim_artist;"
time_table_drop = "drop table if exists dim_time;"

# CREATE TABLES

staging_events_table_create= ("""
create table if not exists stg_events
(
artist varchar,
auth varchar,
firstname varchar,
gender varchar,
iteminsession varchar,
lastname varchar,
length float,
level varchar,
location varchar,
method varchar,
page varchar,
registration float,
sessionid varchar,
song varchar,
status int,
ts bigint,
useragent varchar,
userid int
);
""")

staging_songs_table_create = ("""
create table if not exists stg_songs
(
num_songs int,
artist_id varchar,
artist_latitude varchar,
artist_longitude varchar,
artist_location varchar,
artist_name varchar,
song_id varchar,
title varchar,
duration float,
year int
);
""")

songplay_table_create = ("""
create table if not exists fact_songplay
(
songplay_id int identity(0,1) primary key,
start_time timestamp references dim_time(start_time),
user_id int references dim_user(user_id),
level varchar,
song_id varchar references dim_song(song_id),
artist_id varchar references dim_artist(artist_id),
session_id varchar,
location varchar,
user_agent varchar
);
""")

user_table_create = ("""
create table if not exists dim_user
(
user_id int primary key,
first_name varchar,
last_name varchar,
gender varchar,
level varchar
);
""")

song_table_create = ("""
create table if not exists dim_song
(
song_id varchar primary key,
title varchar,
artist_id varchar,
year int,
duration float
);
""")

artist_table_create = ("""
create table if not exists dim_artist
(
artist_id varchar primary key,
name varchar,
location varchar,
latitude varchar,
longitude varchar);
""")

time_table_create = ("""
create table if not exists dim_time
(
start_time timestamp primary key,
hour int,
day int,
week int,
month int,
year int,
weekday int
);
""")

# STAGING TABLES

staging_events_copy = ("""
copy stg_events from {} credentials 'aws_iam_role={}' format as json {} region 'us-west-2';
""").format(config['S3']['LOG_DATA'],config['IAM_ROLE']['ARN'],config['S3']['LOG_JSONPATH'])

staging_songs_copy = ("""
copy stg_songs from {} credentials 'aws_iam_role={}' format as json 'auto' region 'us-west-2';
""").format(config['S3']['SONG_DATA'],config['IAM_ROLE']['ARN'])

# FINAL TABLES

songplay_table_insert = ("""
insert into fact_songplay (start_time,user_id,level,song_id,artist_id,session_id,location,user_agent)
(select
timestamp 'epoch' + e.ts/1000*interval '1 second' as start_time,
e.userid as user_id,
u.level,
s.song_id,
a.artist_id,
e.sessionid as session_id,
e.location,
e.useragent as user_agent
from stg_events as e
inner join dim_user as u on e.userid=u.user_id
inner join dim_song as s on e.song=s.title
inner join dim_artist as a on e.artist=a.name
where e.page='NextSong'
);
""")

user_table_insert = ("""
insert into dim_user
(select distinct userid,firstname,lastname,gender,level from stg_events where userid is not null
and ts in (select max(ts) from stg_events group by userid)
);
""")

song_table_insert = ("""
insert into dim_song
(select distinct song_id,title,artist_id,year,duration from stg_songs);
""")

artist_table_insert = ("""
insert into dim_artist
(select distinct artist_id,artist_name,artist_location,artist_latitude,artist_longitude from stg_songs);
""")

time_table_insert = ("""
insert into dim_time
(select 
distinct
timestamp 'epoch' + ts/1000*interval '1 second' as start_time,
extract(hour from timestamp 'epoch' + ts/1000*interval '1 second') as hour,
extract(day from timestamp 'epoch' + ts/1000*interval '1 second') as day,
extract(week from timestamp 'epoch' + ts/1000*interval '1 second') as week,
extract(month from timestamp 'epoch' + ts/1000*interval '1 second') as month,
extract(year from timestamp 'epoch' + ts/1000*interval '1 second') as year,
extract(weekday from timestamp 'epoch' + ts/1000*interval '1 second') as weekday
from stg_events
where ts in (select distinct ts from stg_events));
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [user_table_insert, song_table_insert, artist_table_insert, time_table_insert, songplay_table_insert]
