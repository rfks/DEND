# Data Engineering Nano Degree by Udacity
## Project 3 - Data warehouse
### Introduction
A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.
### Project Description
The task is building an ETL pipeline that extracts their data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for their analytics team to continue finding insights in what songs their users are listening to.
### Song Dataset
The first dataset is a subset of real data from the Million Song Dataset. Each file is in JSON format and contains metadata about a song and the artist of that song. The files are partitioned by the first three letters of each song's track ID. For example, here are filepaths to two files in this dataset.

```
song_data/A/B/C/TRABCEI128F424C983.json
song_data/A/A/B/TRAABJL12903CDCF1A.json
```
And below is an example of what a single song file, TRAABJL12903CDCF1A.json, looks like.

```
{"num_songs": 1, "artist_id": "ARJIE2Y1187B994AB7", "artist_latitude": null, "artist_longitude": null, "artist_location": "", "artist_name": "Line Renaud", "song_id": "SOUPIRU12A6D4FA1E1", "title": "Der Kleine Dompfaff", "duration": 152.92036, "year": 0}
```
### Log Dataset
The second dataset consists of log files in JSON format generated by this event simulator based on the songs in the dataset above. These simulate activity logs from a music streaming app based on specified configurations.

The log files in the dataset are partitioned by year and month. For example, here are filepaths to two files in this dataset.

```
log_data/2018/11/2018-11-12-events.json
log_data/2018/11/2018-11-13-events.json
```

### Here are the S3 links for each:

* Song data: `3://udacity-dend/song_data`
* Log data: `s3://udacity-dend/log_data`
* Log data json path: `s3://udacity-dend/log_json_path.json`

## Schema for Song Play Analysis
Using the song and log datasets, you'll need to create a star schema optimized for queries on song play analysis. This includes the following tables.

#### Fact Table
1. **songplays** - records in log data associated with song plays i.e. records with page NextSong
   * _songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent_

#### Dimension Tables
1. **users** - users in the app
   * _user_id, first_name, last_name, gender, level_
2. **songs** - songs in music database
   * _song_id, title, artist_id, year, duration_
3. **artists** - artists in music database
   * _artist_id, name, location, latitude, longitude_
4. **time** - timestamps of records in **songplays** broken down into specific units
   * _start_time, hour, day, week, month, year, weekday_

![alt text](https://github.com/rfks/DEND/blob/master/3_data_warehouse/sparkify_schema.png "schema")

## Project Template
In addition to the data files, the project workspace includes five files:

1. `create_tables.py` is where we'll create our fact and dimension tables for the star schema in Redshift.
2. `etl.py` is where we'll load data from S3 into staging tables on Redshift and then process that data into our analytics tables on Redshift.
3. `sql_queries.py` is where we'll define our SQL statements, which will be imported into the two other files above.
4. `README.md` provides discussion on this project.
5. `dwh.cfg` config file with aws redshift cluster, iam and s3 details. **!!!Update before run.**

## How to run the project

**!!!First the config file needs to be updated with the target redshift cluster access details.** Then the below script can be run in that order:
```sh
root@xyz:home/workspace# python create_tables.py
root@xyz:home/workspace# python etl.py
```

We can check the results connecting to the redshift cluster after the scripts run successfully.
