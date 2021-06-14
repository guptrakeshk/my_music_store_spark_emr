import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, to_date, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, dayofweek
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType


# Read credentials from config file
config = configparser.ConfigParser()
config.read('dl.cfg')

# Set AWS credentials as environment variable to be used by spark application
os.environ['AWS_ACCESS_KEY_ID']=config['spark-admin']['aws_access_key_id']
os.environ['AWS_SECRET_ACCESS_KEY']=config['spark-admin']['aws_secret_access_key']


def create_spark_session():
    """ This function creates or get a Spark session for given application. 
        Specify the jar packages for having specific version while creating Sparksession.
        :return: A SparkSession instance
        :rtype: SparkSession
    """
    spark = SparkSession \
        .builder \
        .appName("Spark_ETL") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.3") \
        .getOrCreate()
    return spark


def get_song_df(spark, input_data):
    """ A function that access S3 bucket to read songs data.
        :param spark: A Spark session 
        :type spark: SparkSession
        :param input_data : S3 location for songs data log
        :type input_data : str
        
        :return: A Spark dataframe 
        :rtype: DataFrame
    """
    song_data = os.path.join(input_data, "song-data/*/*/*/*.json")

    song_data_schema = StructType([
                StructField("song_id", StringType()),
                StructField("title", StringType(), True),
                StructField("artist_id", StringType(), True),
                StructField("year", IntegerType(), True),
                StructField("duration", DoubleType(), True),
                StructField("artist_name", StringType(), True),
                StructField("artist_location", StringType(), True),
                StructField("artist_latitude", DoubleType(), True),
                StructField("artist_longitude", DoubleType(), True),
                StructField("num_songs", IntegerType(), True)
    ])

    # Make sure to provide schema to validate while reading the data from S3 location
    # Caution: Sometime Spark app complains and can't infer schema from the data set. 
    # In such case it throws error.
    # AnalysisException: unable to infer schema for json. it must be specified manually.
    try:
        song_df = spark.read.json(song_data, schema=song_data_schema)
        song_df.show(3)
    except Exception as e:
        print(e)
    
    return song_df

def process_song_data(spark, input_data, output_data):
    """ A function that processes song data log which is store in AWS S3.
        Function access song data log from S3 using AWS credentials.
        It then creates a temp songs_data_table from songs data frame 
        - It select song specific columns from temp table in declartive SQL
        - Function then write songs record data into a parquet file format back to AWS S3
        - It repeats the process for artitst data
        :param spark: spark session 
        :type spark: SparkSession
        :param input_data : S3 location for songs data
        :type input_data : str
        :param output_data: S3 location for writing processed songs data in Parquet files
        :type output_data : str
    """
    
    # Get songs data files and create a data frame
    song_df = get_song_df(spark, input_data)
    
    # Create a temp table song_data_table from songs data frame.
    song_temp_table = song_df.createOrReplaceTempView("song_data_table")

    # Get song records from the song_data_table using declarative SQL style query
    song_table = spark.sql("""
                SELECT song_id, title, artist_id, year, duration
                FROM song_data_table
        """)

    # Write song table data records into parquet files partitioned by year and artist
    song_output_location = os.path.join(output_data,"song_data/")

    try:
        song_table.write.partitionBy("year", "artist_id")\
                      .mode("overwrite")\
                      .parquet(song_output_location)
    except Exception as e :
        print("Exception : Could not write songs data to parquet file ")
        print(e)        
    
    artist_table = spark.sql("""
            SELECT distinct artist_id, artist_name, artist_location, artist_latitude,
                    artist_longitude, num_songs
            FROM song_data_table
            WHERE artist_id IS NOT NULL
    """)
    # Check the content of song data.
    artist_table.show(3)
    
    # Write artist data into parquet file
    artist_output_location = os.path.join(output_data, "artist_data/")
    print("S3 output location for processed artist data : ", artist_output_location)

    try :
        artist_table.write.mode("overwrite")\
                        .parquet(artist_output_location)
    except Exception as e :
        print("Exception : Could not write artists data to parquet file")
        print(e)


def process_log_data(spark, input_data, output_data):
    """ A function that processes events log which is stored in AWS S3.
        Function access events log from S3 using AWS credentials.
        
        :param spark: spark session 
        :type spark: SparkSession
        :param input_data : S3 location for songs data
        :type input_data : str
        :param output_data: S3 location for writing processed songs data in Parquet files
        :type output_data : str
    """
    # get filepath to log data file
    log_data = os.path.join(input_data, "log_data/2018/11/*.json")
    
    # read log data file
    try:
        df = spark.read.json(log_data)
    except Exception as e:
        print("Exception: Could not read log-data from S3")
        print(e)

    # filter by actions for song plays

    df = df.filter(df.page == 'NextSong')
    log_temp_table = df.createOrReplaceTempView("log_data_table")

    # extract columns for users table    
    users_table = spark.sql("""
                SELECT distinct userId, firstName, lastName, gender, level
                FROM log_data_table
    """)
    
    # write users table to parquet files
    try:
        users_outout_location = os.path.join(output_data, "users_data/")
        users_table.write.mode("overwrite")\
                        .parquet(users_outout_location)
    except Exception as e:
        print("Exception : Error in writing to parquet file")
        print(e)
        
        

    # create timestamp column from original timestamp column in dataframe using Python 
    # datatime function via UDF
    get_timestamp = udf(lambda x: datetime.fromtimestamp(x/1000.0), TimestampType() )

    df = df.withColumn("start_time", get_timestamp(df.ts))

    df = df.withColumn("hour", hour(df.start_time))
    df = df.withColumn("day", dayofmonth(df.start_time))
    df = df.withColumn("weekday", dayofweek(df.start_time))
    df = df.withColumn("month", month(df.start_time))
    df = df.withColumn("weekofyear", weekofyear(df.start_time))
    df = df.withColumn("year", year(df.start_time))


    # extract columns to create time table

    time_table = df.select(col('start_time'), col('hour'), col('day'), col('weekday'), col('month'),\
                           col('weekofyear'), col('year')).distinct()

    # write time table to parquet files partitioned by year and month
    try :
        time_output_location = os.path.join(output_data, "time_data/")
        time_table.write.partitionBy("year", "month")\
                        .mode("overwrite")\
                        .parquet(time_output_location)
    except Exception as e:
        print("Exception: Error in writing time data to parquet file")
        print(e)
    
    # Get the songs data frame
    song_df = get_song_df(spark, input_data)
    
    # read in song data to use for songplays table
    formatted_dict = {"start_time":"start_time", "userId":"user_id", "level":"level", "song_id":"song_id",\
                "artist_id":"artist_id","sessionId":"session_id", "location":"location", "userAgent":"user_agent"}

    unpacked_cols = ["{} as {}".format(cols,item) for cols, item in formatted_dict.items()]
    
    # Apply inner join between songs data frame and events log data frame. 
    # Format column names with desired column name from dictionary
    # Also create new auto increment column songplay_id.
    songplays_table = df.join(song_df, song_df.artist_name == df.artist, "inner") \
            .selectExpr(*unpacked_cols) \
            .withColumn("songplay_id", monotonically_increasing_id())


    # write songplays table to parquet files partitioned by year and month
    try :
        songplays_output_location = os.path.join(output_data, "songplays_data/")
        time_table.write.partitionBy("year", "month") \
                        .mode("overwrite") \
                        .parquet(songplays_output_location)
    except Exception as e:
        print("Exception: Error in writing song plays data to parquet file")
        print(e)


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://rakeshg-us-west-2-bucket/" 
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
