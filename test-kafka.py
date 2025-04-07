from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, min, max, avg, year, month, sum, date_format, concat, lit, regexp_replace, lower
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType, TimestampType
import time
import os
import boto3
from botocore.client import Config

# Get absolute path to the jars directory
jars_dir = os.path.abspath("./jars")
jars = ",".join([
    f"{jars_dir}/hadoop-aws-3.3.1.jar",
    f"{jars_dir}/aws-java-sdk-bundle-1.11.901.jar",
    f"{jars_dir}/wildfly-openssl-1.0.7.Final.jar"
])

# MinIO connection parameters
endpoint_url = "http://localhost:9000"
access_key = "myminioadmin"
secret_key = "myminioadmin"


# Create a Spark session + MinIO configuration
spark = SparkSession.builder \
    .appName("Kafka to MinIO Household Data") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0") \
    .config("spark.hadoop.fs.s3a.endpoint", endpoint_url) \
    .config("spark.hadoop.fs.s3a.access.key", access_key) \
    .config("spark.hadoop.fs.s3a.secret.key", secret_key) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.jars", jars) \
    .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2") \
    .config("spark.sql.sources.commitProtocolClass", "org.apache.spark.sql.execution.datasources.SQLHadoopMapReduceCommitProtocol") \
    .getOrCreate()

# Define Kafka source parameters
kafka_broker = "localhost:9092"
topic = "household_data"

# Define schema for the household data
household_schema = StructType([
    StructField("household_id", StringType(), nullable=False),
    StructField("timestamp", TimestampType(), nullable=False),
    StructField("electricity_usage_kwh", FloatType(), nullable=False),
    StructField("voltage", IntegerType(), nullable=False),
    StructField("current", FloatType(), nullable=False),
    StructField("location", StructType([
        StructField("house_number", StringType(), nullable=False),
        StructField("ward", StringType(), nullable=False),
        StructField("district", StringType(), nullable=False)
    ]), nullable=False),
    StructField("price_per_kwh", IntegerType(), nullable=False),
    StructField("total_cost", IntegerType(), nullable=False)
])

# Read data from Kafka
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_broker) \
    .option("subscribe", topic) \
    .option("startingOffsets", "earliest") \
    .option("maxOffsetsPerTrigger", 500) \
    .load()

# Convert the Kafka value column (binary) to a string and parse the JSON
household_df = kafka_df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), household_schema).alias("data")) \
    .select("data.*")
    
household_df.printSchema()

# Extract year and month from the timestamp
household_df = household_df.withColumn("year", year("timestamp")) \
                          .withColumn("month", month("timestamp")) \
                          .withColumn("electricity_usage_kwh", col("electricity_usage_kwh").cast(FloatType())) \
                          .withColumn("formatted_timestamp", date_format("timestamp", "yyyy-MM-dd HH-mm"))

# Create bucket_name column for raw data with format: district-ward-house_number (lowercase)
household_df = household_df.withColumn(
    "bucket_name", 
    lower(
        concat(
            regexp_replace(col("location.district"), " ", "-"), lit("-"),
            regexp_replace(col("location.ward"), " ", "-"), lit("-"),
            regexp_replace(col("location.house_number"), " ", "-")
        )
    )
)

s3_client = boto3.client(
    's3',
    endpoint_url=endpoint_url,
    aws_access_key_id=access_key,
    aws_secret_access_key=secret_key,
    config=Config(signature_version='s3v4'),
    region_name='us-east-1'  # Required by boto3, not used by MinIO
)


# # Function to check if bucket exists and create if it doesn't
# def ensure_bucket_exists(bucket_name):
#     try:
#         # Check if bucket exists
#         existing_buckets = s3_client.list_buckets()
#         bucket_exists = any(bucket['Name'] == bucket_name for bucket in existing_buckets['Buckets'])
        
#         if not bucket_exists:
#             print(f"Bucket '{bucket_name}' does not exist. Creating it...")
#             s3_client.create_bucket(Bucket=bucket_name)
#             print(f"Bucket '{bucket_name}' created successfully.")
#         else:
#             print(f"Bucket '{bucket_name}' already exists.")
            
#         return True
#     except Exception as e:
#         print(f"Error checking/creating bucket: {e}")
#         return False

# Function to write raw household data to MinIO with bucket verification
def write_raw_data_to_minio(batch_df, batch_id):
    # Get unique bucket names
    bucket_names = [row.bucket_name for row in batch_df.select("bucket_name").distinct().collect()]
    
    for bucket_name in bucket_names:
        # Ensure bucket exists before writing data
        if ensure_bucket_exists(bucket_name):
            # Filter data for this bucket
            bucket_data = batch_df.filter(col("bucket_name") == bucket_name)
            
            # Select only the relevant columns for raw data storage
            output_data = bucket_data.select(
                "electricity_usage_kwh", 
                "formatted_timestamp",
                "location",
                "household_id"
            )
            timestamp = int(time.time())
            # Create path for the data
            file_path = f"s3a://{bucket_name}/{timestamp}.txt"
            
            try:
                # Convert to single string column for text format
                from pyspark.sql.functions import concat_ws
                text_data = output_data.select(
                    concat_ws(',', col("household_id"),col("location"), col("electricity_usage_kwh"), col("formatted_timestamp")).alias("value")
                )
                
                # Write as text file
                text_data.coalesce(1).write.mode("append").text(file_path)
                print(f"Successfully wrote raw data to {file_path}")
            except Exception as e:
                print(f"Error writing to {file_path}: {e}")
        else:
            print(f"Skipping writing to bucket '{bucket_name}' as it could not be created or verified.")


 # Create bucket_name column for aggregated data with format: district-ward-house_number_year_month_average



# Group by household_id, year, and month
ward_df = household_df.groupBy("household_id", "year", "month", "location.district", "location.ward") \
    .agg(
        min("electricity_usage_kwh").alias("min_electricity_usage_kwh"),
        max("electricity_usage_kwh").alias("max_electricity_usage_kwh"),
        avg("electricity_usage_kwh").alias("avg_electricity_usage_kwh"),
        min("voltage").alias("min_voltage"),
        max("voltage").alias("max_voltage"),
        avg("voltage").alias("avg_voltage")
    )
    
ward_df = ward_df.withColumn(
    "bucket_name",
    lower( 
        concat(
            regexp_replace(col("district"), " ", "-"), lit("-"),
            regexp_replace(col("ward"), " ", "-"), lit("-"),
            col("year"), lit("-"),
            col("month")
        )
    )
)

# Function to write aggregated household data to MinIO
def write_aggregated_data_to_minio(batch_df, batch_id):
    # Get unique bucket names 
    batch_count = batch_df.count()
    print(f"Batch {batch_id} contains {batch_count} records")
    bucket_names = [row.bucket_name for row in batch_df.select("bucket_name").distinct().collect()]
    for bucket_name in bucket_names:
        # if ensure_bucket_exists(bucket_name):
            # Filter data for this bucket
            bucket_data = batch_df.filter(col("bucket_name") == bucket_name)
            first_row = bucket_data.first()
            year = str(first_row["year"])
            month = str(first_row["month"])
            # Select only the relevant columns for aggregated data storage
            output_data = bucket_data.select(
                "min_electricity_usage_kwh",
                "max_electricity_usage_kwh", 
                "avg_electricity_usage_kwh",
                "year",
                "month"
            )
            
            # Create path for the data
            file_path = f"s3a://monthly/{bucket_name}/{year}/{month}.txt"
            
            try:
                # Convert to single string column for text format
                from pyspark.sql.functions import concat_ws
                text_data = output_data.select(
                    concat_ws(',', 
                        col("min_electricity_usage_kwh"), 
                        col("max_electricity_usage_kwh"), 
                        col("avg_electricity_usage_kwh"), 
                        col("year"), col("month")
                    ).alias("value")
                )
                # Write as text file
                text_data.coalesce(1).write.mode("overwrite").text(file_path)
                print(f"Successfully wrote aggregated data to {file_path}")
            except Exception as e:
                print(f"Error writing to {file_path}: {e}")
                
        # else:
        #     print(f"Skipping writing to bucket '{bucket_name}' as it could not be created or verified.")


# household_query = household_df.writeStream \
#     .outputMode("append") \
#     .foreachBatch(write_raw_data_to_minio) \
#     .start()
    
# console_household_query = household_df.writeStream \
#     .outputMode("append") \
#     .format("console") \c lear
#     .option("truncate", "false") \
#     .start()

ward_query = ward_df.writeStream \
    .outputMode("complete") \
    .foreachBatch(write_aggregated_data_to_minio) \
    .start()
    
# console_ward_query = ward_df.writeStream \
#     .outputMode("complete") \
#     .format("console") \
#     .option("truncate", "false") \
#     .start()
    
# Wait for the queries to terminate
# console_household_query.awaitTermination()
# raw_data_query.awaitTermination()
ward_query.awaitTermination()
console_ward_query.awaitTermination()