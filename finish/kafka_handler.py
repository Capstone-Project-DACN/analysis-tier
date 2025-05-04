# Kafka_handler.py
import os
from pyspark.sql.functions import from_json, col, year, month, date_format, concat, lit, regexp_extract, lower,hour,minute
from pyspark.sql.types import FloatType

# Kafka connection parameters
KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "localhost:9092")
KAFKA_HOUSEHOLD_TOPIC = os.environ.get("KAFKA_HOUSEHOLD_TOPIC", "household_data")
KAFKA_AREA_TOPIC = os.environ.get("KAFKA_AREA_TOPIC", "area_data")
KAFKA_MAX_OFFSET = os.environ.get("KAFKA_MAX_OFFSET", "500")

def create_household_stream(spark, schema):
    """Create Spark streaming DataFrame for household data from Kafka"""
    # Read household data from Kafka
    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("subscribe", KAFKA_HOUSEHOLD_TOPIC) \
        .option("startingOffsets", "latest") \
        .option("maxOffsetsPerTrigger", KAFKA_MAX_OFFSET) \
        .option("failOnDataLoss", "false") \
        .option("kafka.group.id", "analysis-tier-household") \
        .load()

    # Parse the JSON data
    household_df = kafka_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), schema).alias("data")) \
        .select("data.*")
    
    # Add computed columns
    household_df = household_df.withColumn("year", year("timestamp")) \
                                .withColumn("month", month("timestamp")) \
                                .withColumn("hour", hour(col("timestamp"))) \
                                .withColumn("minute", minute(col("timestamp")))\
                                .withColumn("date_part", date_format(col("timestamp"), "yyyy-MM-dd")) \
                                .withColumn("electricity_usage_kwh", col("electricity_usage_kwh").cast(FloatType())) \
                                .withColumn("formatted_timestamp", date_format("timestamp", "yyyy-MM-dd HH-mm-ss"))

    # Extract city, district, and house_number from device_id
    # Example: "household-HCMC-Q1-261" -> "hcmc-q1-261"

    
    return household_df

def create_ward_stream(spark, schema):
    """Create Spark streaming DataFrame for ward/area data from Kafka"""
    # Read area data from Kafka
    area_kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("subscribe", KAFKA_AREA_TOPIC) \
        .option("startingOffsets", "latest") \
        .option("maxOffsetsPerTrigger", KAFKA_MAX_OFFSET) \
        .option("failOnDataLoss", "false") \
        .option("kafka.group.id", "analysis-tier-area") \
        .load()

    # Parse the area data JSON
    area_df = area_kafka_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), schema).alias("data")) \
        .select("data.*")

    # Add formatted timestamp and create ward-based file path
    area_df = area_df.withColumn("year", year("timestamp")) \
                    .withColumn("month", month("timestamp")) \
                    .withColumn("hour", hour(col("timestamp"))) \
                    .withColumn("minute", minute(col("timestamp")))\
                    .withColumn("date_part", date_format(col("timestamp"), "yyyy-MM-dd")) \
                    .withColumn("formatted_timestamp", date_format("timestamp", "yyyy-MM-dd HH-mm-ss")) \
                    .withColumn("total_electricity_usage_kwh", col("total_electricity_usage_kwh").cast(FloatType()))

    # Create ward-based bucket path for storage
    # Example: "area-HCMC-Q1" -> "hcmc-q1"
    area_df = area_df.withColumn(
        "bucket_name", 
        lower(
            concat(
                regexp_extract(col("device_id"), "area-([^-]+)-([^-]+).*", 1), lit("-"),
                regexp_extract(col("device_id"), "area-([^-]+)-([^-]+).*", 2)
            )
        )
    )
    
    return area_df

def start_streaming_queries(household_df, area_df, processor_func, spark, buckets):
    """Start streaming queries for both household and ward data
    
    Parameters:
    -----------
    household_df : DataFrame
        Household data stream
    area_df : DataFrame
        Ward data stream
    processor_func : function
        The function to process each batch of data
    spark : SparkSession
        The Spark session
    buckets : dict
        Dictionary mapping data types to bucket names
    """
    from functools import partial
    
    # Create specialized processor functions with pre-filled parameters
    household_processor = lambda batch_df, batch_id: processor_func(
        batch_df, batch_id, 'household', spark, buckets['household']
    )
    
    ward_processor = lambda batch_df, batch_id: processor_func(
        batch_df, batch_id, 'ward', spark, buckets['ward']
    )
    
    # Start the household streaming query
    household_query = household_df.writeStream \
        .outputMode("append") \
        .option("checkpointLocation", "/tmp/checkpoint/household_data") \
        .foreachBatch(household_processor) \
        .trigger(processingTime="10 seconds") \
        .start()
    
    # Start the ward streaming query
    ward_query = area_df.writeStream \
        .outputMode("append") \
        .option("checkpointLocation", "/tmp/checkpoint/ward_data") \
        .foreachBatch(ward_processor) \
        .trigger(processingTime="10 seconds") \
        .start()
    
    return household_query, ward_query