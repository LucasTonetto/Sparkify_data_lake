import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, from_unixtime
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import TimestampType, DateType, StructType, StringType, IntegerType, DoubleType, StructField


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['KEYS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['KEYS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    This function is responsable for create spark's session.
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.5") \
        .config("spark.hadoop.fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.awsAccessKeyId", os.environ['AWS_ACCESS_KEY_ID']) \
        .config("spark.hadoop.fs.s3a.awsSecretAccessKey", os.environ['AWS_SECRET_ACCESS_KEY']) \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    This function is responsable for proccess song_data files and save them into the output.

    INPUTS: 
    * spark: spark session
    * input_data: path where song_data is saved
    * output_data: path where process song data will be save
    """
    # get filepath to song data file
    song_data = input_data + '/song_data/A/A/A/*.json'
    
    # read song data file
    song_schema = StructType([
        StructField("artist_id", StringType(), True), \
        StructField("artist_latitude", DoubleType(), True), \
        StructField("artist_location", StringType(), True), \
        StructField("artist_longitude", DoubleType(), True), \
        StructField("artist_name", StringType(), True), \
        StructField("duration", DoubleType(), True), \
        StructField("num_songs", IntegerType(), True), \
        StructField("song_id", StringType(), True), \
        StructField("title", StringType(), True), \
        StructField("year", IntegerType(), True), \
    ])
    df = spark.read.json(song_data, schema=song_schema)

    # extract columns to create songs table
    songs_table = df.select(\
        col("song_id"), \
        col("title"), \
        col("year"), \
        col("duration"), \
        col("artist_id") \
    ).dropDuplicates(['song_id'])
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode("overwrite").partitionBy('year', 'artist_id').parquet(os.path.join(output_data, 'songs'))

    # extract columns to create artists table
    artists_table = df.select(\
        col("artist_id"), \
        col("artist_name").alias("name"), \
        col("artist_location").alias("location"), \
        col("artist_latitude").alias("latitude"), \
        col("artist_longitude").alias("longitude"), \
    ).dropDuplicates(['artist_id'])
    
    # write artists table to parquet files
    artists_table.write.mode("overwrite").parquet(os.path.join(output_data, 'artists'))


def process_log_data(spark, input_data, output_data):
    """
    This function is responsable for proccess log_data files and save them into the output.

    INPUTS: 
    * spark: spark session
    * input_data: path where log_data is saved
    * output_data: path where process log data will be save
    """
    # get filepath to log data file
    log_data = input_data+'/log-data/*.json'

    # read log data file
    log_schema = StructType([
        StructField("artist", StringType(), True), \
        StructField("auth", StringType(), True), \
        StructField("firstName", StringType(), True), \
        StructField("gender", StringType(), True), \
        StructField("itemInSession", IntegerType(), True), \
        StructField("lastName", StringType(), True), \
        StructField("length", DoubleType(), True), \
        StructField("level", StringType(), True), \
        StructField("location", StringType(), True), \
        StructField("method", StringType(), True), \
        StructField("page", StringType(), True), \
        StructField("registration", DoubleType(), True), \
        StructField("sessionId", IntegerType(), True), \
        StructField("song", StringType(), True), \
        StructField("status", IntegerType(), True), \
        StructField("ts", IntegerType(), True), \
        StructField("userAgent", StringType(), True), \
        StructField("userId", IntegerType(), True), \
    ])
    df = spark.read.json(log_data, schema=log_schema)
    
    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')

    # extract columns for users table
    users_table = df.select(\
        col("userId").alias("user_id"), \
        col("firstName").alias("first_name"), \
        col("lastName").alias("last_name"), \
        col("gender"), \
        col("level") \
    ).where(col('userId') != '').dropDuplicates(['user_id'])
    
    # write users table to parquet files
    users_table.write.mode("overwrite").parquet(os.path.join(output_data, 'users'))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: datetime.fromtimestamp((x/1000.0)), TimestampType())
    df = df.withColumn('datetime', get_datetime('ts'))
    
    # extract columns to create time table
    time_table = df.select(\
        col("ts").alias('start_time'), \
        date_format('datetime', 'k').alias('hour'),\
        date_format('datetime', 'd').alias('day'),\
        date_format('datetime', 'w').alias('week'),\
        date_format('datetime', 'M').alias('month'),\
        date_format('datetime', 'Y').alias('year'),\
        date_format('datetime', 'u').alias('weekday'),\
    ).dropDuplicates(['start_time'])
    
    # write time table to parquet files partitioned by year and month
    time_table.write.mode("overwrite").parquet(os.path.join(output_data, 'time'))

    # read in song data to use for songplays table
    song_schema = StructType([
        StructField("artist_id", StringType(), True), \
        StructField("artist_latitude", DoubleType(), True), \
        StructField("artist_location", StringType(), True), \
        StructField("artist_longitude", DoubleType(), True), \
        StructField("artist_name", StringType(), True), \
        StructField("duration", DoubleType(), True), \
        StructField("num_songs", IntegerType(), True), \
        StructField("song_id", StringType(), True), \
        StructField("title", StringType(), True), \
        StructField("year", IntegerType(), True), \
    ])
    song_df = spark.read.json(input_data + '/song_data/A/A/A/*.json', schema=song_schema)

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = df.join( \
            song_df, \
            (df.song == song_df.title) & \
            (df.artist == song_df.artist_name) & \
            (df.length == song_df.duration), 'left_outer')\
        .select( \
            df.level,\
            col('artist_location').alias('location'), \
            col('userAgent').alias('user_agent'), \
            col('sessionId').alias('session_id'), \
            song_df.artist_id, \
            col('userId').alias('user_id'), \
            song_df.song_id, \
            col('ts').alias('start_time'), \
            year('datetime').alias('year'), \
            month('datetime').alias('month')
        )

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.mode("overwrite").partitionBy('year', 'month').parquet(os.path.join(output_data, 'songplays'))


def main():
    """
    This function is responsible for create spark session,
    get the log and song data into input path, 
    process them and save them into output path
    """
    spark = create_spark_session()
    input_data = ""
    output_data = ""
    
    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
