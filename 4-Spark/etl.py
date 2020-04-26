import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, to_timestamp


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    create spark session 
    
    Returns:
        SparkSession object
    """
    
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Process song_data from input_data path and save songs and artists tables in paquet format in output_data path
    
    Parameters:
        spark: SparkSession object to process data
        input_data: path to input data
        output_data: path to output data
    """
    
    # get filepath to song data file
    song_data = input_data + 'song_data/*/*/*'
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df[['song_id', 'title', 'artist_id', 'year', 'duration']]
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.where(songs_table.song_id.isNotNull()).distinct().write.partitionBy('year', 'artist_id') \
                .mode('overwrite').parquet(output_data + 'songs/')

    # extract columns to create artists table
    artists_table = df[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']]\
                        .selectExpr('artist_id', \
                                    'artist_name as name', \
                                    'artist_location as location', \
                                    'artist_latitude as latitude', \
                                    'artist_longitude as longitude')
    
    # write artists table to parquet files
    artists_table.where(artists_table.artist_id.isNotNull()).distinct().write.mode('overwrite').parquet(output_data + 'artists/')


def process_log_data(spark, input_data, output_data):
    """
    Process log_data from input_data path and save users, time and songplays tables in paquet format in output_data path
    
    Parameters:
        spark: SparkSession object to process data
        input_data: path to input data
        output_data: path to output data
    """
    
    # get filepath to log data file
    log_data = input_data + 'log_data/*'

    # read log data file
    log_df = spark.read.json(log_data)
    
    # filter by actions for song plays 
    log_df = log_df.filter('page = "NextSong"') \
                   .withColumn('user_id', log_df['userId'].cast('integer')) \
                   .withColumn('session_id', log_df['sessionId'].cast('integer')) \
                   .withColumnRenamed('firstName', 'first_name') \
                   .withColumnRenamed('lastName', 'last_name')

    # extract columns for users table    
    users_table = log_df[['user_id', 'first_name', 'last_name', 'gender', 'level']]
    
    # write users table to parquet files
    users_table.where(users_table.user_id.isNotNull()).distinct().write.mode('overwrite').parquet(output_data + 'users/')

    # create timestamp column from original timestamp column
    time_df = log_df[['ts']]
    
    # create datetime column from original timestamp column
    time_df = time_df.withColumn('ts', to_timestamp(col('ts')/1000))
    
    # extract columns to create time table
    time_table = time_df.withColumnRenamed('ts', 'start_time') \
                        .withColumn('hour', hour(col('start_time'))) \
                        .withColumn('day', dayofmonth(col('start_time'))) \
                        .withColumn('week', weekofyear(col('start_time'))) \
                        .withColumn('month', month(col('start_time'))) \
                        .withColumn('year', year(col('start_time'))) \
                        .withColumn('weekday', date_format(col('start_time'), 'u').cast('integer'))
     
    # write time table to parquet files partitioned by year and month
    time_table.distinct().write.partitionBy('year', 'month').mode('overwrite').parquet(output_data + 'time/')

    # read in song data to use for songplays table
    song_df = spark.read.json(input_data + 'song_data/*/*/*')

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = log_df.join(song_df, [log_df.song == song_df.title, log_df.artist == song_df.artist_name]) \
                             .selectExpr('monotonically_increasing_id() as songplay_id', \
                                         'to_timestamp(ts/1000) as start_time', \
                                         'month(to_timestamp(ts/1000)) as month', \
                                         'year(to_timestamp(ts/1000)) as year', \
                                         'user_id as user_id', \
                                         'level as level', \
                                         'song_id as song_id', \
                                         'artist_id as artist_id', \
                                         'session_id as session_id', \
                                         'location as location', \
                                         'userAgent as user_agent') 

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.mode('overwrite').partitionBy('year', 'month').parquet(output_data+'songplays/')


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = ""
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
