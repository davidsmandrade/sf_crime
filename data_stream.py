import logging
import json
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as psf


#Create a schema for incoming resources
schema = StructType([
        StructField("crime_id", StringType(), False),
        StructField("original_crime_type_name", StringType(), True),
        StructField("report_date", TimestampType(), True),
        StructField("call_date", TimestampType(), True),
        StructField("offense_date", TimestampType(), True),
        StructField("call_time", StringType(), True),
        StructField("call_date_time", TimestampType(), True),
        StructField("disposition", StringType(), True),
        StructField("address", StringType(), True),
        StructField("city", StringType(), True),
        StructField("state", StringType(), True),
        StructField("agency_id", StringType(), True),
        StructField("address_type", StringType(), True),
        StructField("common_location", StringType(), True)
])

def run_spark_job(spark):

    # Create Spark Configuration
    # Create Spark configurations with max offset of 200 per trigger
    # set up correct bootstrap server and port
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "com.udacity.crime.police-event") \
        .option("startingOffsets", "earliest") \
        .option("maxOffsetsPerTrigger", 200) \
        .option("stopGracefullyOnShutdown", "true") \
        .load()

    # Show schema for the incoming resources for checks
    logging.debug("Printing schema of incoming data")
    df.printSchema()

    # extract the correct column from the kafka input resources
    # Take only value and convert it to String
    kafka_df = df.selectExpr("CAST(value AS STRING)")

    service_table = kafka_df\
        .select(psf.from_json(psf.col('value'), schema).alias("DF"))\
        .select("DF.*")

    # select original_crime_type_name and disposition
    distinct_table = service_table \
        .select("original_crime_type_name", "disposition", "call_date_time") \
        .distinct()

    # count the number of original crime type
    agg_df = distinct_table \
        .dropna() \
        .select("original_crime_type_name","call_date_time") \
        .withWatermark("call_date_time", "90 minutes") \
        .groupby("original_crime_type_name").count().sort("count", ascending=False)

    # Q1. Submit a screen shot of a batch ingestion of the aggregation
    # write output stream
    logger.info("Streaming crime types and descriptions")
    query = agg_df \
        .writeStream \
        .format('console') \
        .outputMode('Complete') \
        .trigger(processingTime="10 seconds") \
        .option("truncate", "false") \
        .start()


    #attach a ProgressReporter
    print('=== awaitTermination')
    query.awaitTermination()

    print('=== radio_code....')

    # get the right radio code json path
    logger.debug("Reading static data from disk")
    radio_code_json_filepath = "radio_code.json"
    radio_code_df = spark.read.json(radio_code_json_filepath)

    # clean up your data so that the column names match on radio_code_df and agg_df
    # we will want to join on the disposition code

    #rename disposition_code column to disposition
    radio_code_df = radio_code_df.withColumnRenamed("disposition_code", "disposition")

    #join on disposition column
    logger.debug("Joining aggregated data and radio codes")
    join_query = agg_df \
        .join(radio_code_df, col("agg_df.disposition") == col("radio_code_df.disposition"), "left_outer")


    join_query.awaitTermination()


if __name__ == "__main__":
    logger = logging.getLogger(__name__)

    # Create Spark in Standalone mode
    spark = SparkSession \
        .builder \
        .config("spark.ui.port", 3000) \
        .master("local[*]") \
        .appName("KafkaSparkStructuredStreaming") \
        .getOrCreate()

    logger.info("Spark started")
    run_spark_job(spark)

    logger.info("Closing Spark Session")
    spark.stop()
