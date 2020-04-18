import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, dayofweek, date_format
from pyspark.sql.types import TimestampType
from pyspark.sql.functions import monotonically_increasing_id

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
        This function simply makes the connection to the AWS EMR cluster where spark supposed to be installed.
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.8.5") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
        This function processes the song_data from s3 and saves songs and artists details as parquet files.
    """
    
    # get filepath to song data file
    song_data = input_data+'song_data/*/*/*'
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select('song_id','title','artist_id','year','duration').distinct()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode('overwrite').partitionBy('year','artist_id').parquet(output_data+'songs.parquet')

    # extract columns to create artists table
    artists_table = df.select('artist_id','artist_name','artist_location','artist_latitude','artist_longitude').distinct()
    
    # write artists table to parquet files
    artists_table.write.mode('overwrite').parquet(output_data+'artists.parquet')

    
def process_log_data(spark, input_data, output_data):
    """
        This module processes the log_data from s3 and saves users and time details as parquet files.
        After that it combines the event logs with the song_data and creates the songplays output file also in parquet format.
    """
        
    # get filepath to log data file
    log_data = input_data+'log_data'

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter('page=="NextSong"')

    # extract columns for users table    
    users_table = df.select('userid','firstname','lastname','gender','level').distinct()
    
    # write users table to parquet files
    users_table.write.mode('overwrite').parquet(output_data+'users.parquet')

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: datetime.fromtimestamp(x / 1000.0),TimestampType())
    df = df.withColumn('timestamp',get_timestamp('ts'))
    
    # create datetime column from original timestamp column -- Don't need this
    #get_datetime = udf()
    #df = 

    # extract columns to create time table
    time_table = df.select('timestamp') \
        .withColumnRenamed('timestamp','start_time') \
        .distinct() \
        .withColumn('hour',hour(col('start_time'))) \
        .withColumn('day',dayofmonth(col('start_time'))) \
        .withColumn('week',weekofyear(col('start_time'))) \
        .withColumn('month',month(col('start_time'))) \
        .withColumn('year',year(col('start_time'))) \
        .withColumn('weekday',dayofweek(col('start_time')))
    
    # write time table to parquet files partitioned by year and month
    time_table.write.mode('overwrite').partitionBy('year','month').parquet(output_data+'time.parquet')

    # read in song data to use for songplays table
    song_df = spark.read.json(input_data+'song_data/*/*/*')

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = df.join(song_df,(df.song == song_df.title) & (df.length == song_df.duration) & (df.artist == song_df.artist_name),'inner') \
        .withColumn('songplays_id',monotonically_increasing_id()) \
        .withColumn('start_time',get_timestamp('ts')) \
        .select('songplays_id','start_time','userid','level','song_id','artist_id','sessionid','location','useragent') \
        .withColumnRenamed('userid','user_id') \
        .withColumnRenamed('sessionid','session_id') \
        .withColumnRenamed('useragent','user_agent') \
        .withColumn('month',month(col('start_time'))) \
        .withColumn('year',year(col('start_time')))
    
    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.mode('overwrite').partitionBy('year','month').parquet(output_data+'songplays.parquet')
    
    
def main():
    """
        This is the main module. Don't forget to add the target S3 bucket name!
    """
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    
    # add the target S3 bucket details where the parquet files will be written:
    output_data = ""
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
