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
    .config("spark.sql.shuffle.partitions", "10") \
    .config("spark.default.parallelism", "10") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.hadoop.fs.s3a.fast.upload", "true") \
    .config("spark.hadoop.fs.s3a.fast.upload.buffer", "bytebuffer") \
    .config("spark.hadoop.fs.s3a.multipart.size", "5242880") \
    .config("spark.hadoop.fs.s3a.threads.max", "20") \
    .config("spark.hadoop.fs.s3a.connection.maximum", "30") \
    .config("spark.hadoop.fs.s3a.connection.establish.timeout", "5000") \
    .config("spark.hadoop.fs.s3a.connection.timeout", "10000") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", 
           "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

s3_client = boto3.client(
    's3',
    endpoint_url=endpoint_url,
    aws_access_key_id=access_key,
    aws_secret_access_key=secret_key,
    config=Config(signature_version='s3v4'),
    region_name='us-east-1'  # Required by boto3, not used by MinIO
)

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
            regexp_replace(col("location.house_number"), " ", "-"),
        )
    )
)


# Create a 'monthly' bucket at startup
main_bucket = "daily"

try:
    s3_client.head_bucket(Bucket=main_bucket)
    print(f"Main bucket '{main_bucket}' already exists")
except:
    try:
        s3_client.create_bucket(Bucket=main_bucket)
        print(f"Created main bucket '{main_bucket}'")
    except Exception as e:
        print(f"Error creating bucket: {e}")
        
# Function to write raw household data to MinIO with bucket verification
# def write_raw_data_to_minio(batch_df, batch_id):
#     # Get unique bucket names
#     bucket_names = [row.bucket_name for row in batch_df.select("bucket_name").distinct().collect()]
    
#     for bucket_name in bucket_names:
#         # Filter data for this bucket
#         bucket_data = batch_df.filter(col("bucket_name") == bucket_name)
        
#         # Select only the relevant columns for raw data storage
#         output_data = bucket_data.select(
#             "electricity_usage_kwh", 
#             "formatted_timestamp",
#             "location",
#             "household_id"
#         )
#         timestamp = int(time.time())
#         # Create path for the data
#         file_path = f"s3a://{bucket_name}/{formatted_timestamp}"
        
#         try:
#             # Convert to single string column for text format
#             from pyspark.sql.functions import concat_ws
#             text_data = output_data.select(
#                 concat_ws(',', col("household_id"),col("location"), col("electricity_usage_kwh"), col("formatted_timestamp")).alias("value")
#             )
            
#             # Write as text file
#             text_data.coalesce(1).write.mode("append").text(file_path)
#             print(f"Successfully wrote raw data to {file_path}")
#         except Exception as e:
#             print(f"Error writing to {file_path}: {e}")

def write_raw_data_to_minio(batch_df, batch_id):
    """Write raw household data to MinIO using CSV format with timestamp in the filename"""
    start_time = time.time()
    batch_count = batch_df.count()
    
    if batch_count == 0:
        print(f"Batch {batch_id}: No data to process")
        return
        
    print(f"Batch {batch_id}: Processing {batch_count} records")
    
    # Loop through each row in the batch
    for row in batch_df.collect():
        # Extract values from each row
        bucket_name = row["bucket_name"]
        formatted_ts = row["formatted_timestamp"].replace(":", "-").replace(" ", "-")
        household_id = row["household_id"]
        electricity_usage = row["electricity_usage_kwh"]
        voltage = row["voltage"]
        current = row["current"]
        year = row["year"]
        month = row["month"]
        
        # Create file path for this record
        file_path = f"s3a://{main_bucket}/{bucket_name}/{formatted_ts}"
        
        # Create a new small dataframe with just this record
        data = [(household_id, electricity_usage, voltage, current, formatted_ts,)]
        columns = ["household_id", "electricity_usage_kwh", "voltage", "current", 
                   "formatted_timestamp",]
        record_df = spark.createDataFrame(data, columns)
        
        try:
            # Write this record
            record_df.write \
                .mode("append") \
                .option("header", "true") \
                .csv(file_path)
        except Exception as e:
            print(f"Error writing record {household_id} to {file_path}: {e}")
    
    elapsed = time.time() - start_time
    records_per_second = batch_count / elapsed if elapsed > 0 else 0
    print(f"Batch {batch_id}: Completed in {elapsed:.2f} seconds ({records_per_second:.2f} records/sec)")
    
    
household_query = household_df.writeStream \
    .outputMode("append") \
    .foreachBatch(write_raw_data_to_minio) \
    .start()
    
# console_household_query = household_df.writeStream \
#     .outputMode("append") \
#     .format("console") \
#     .option("truncate", "false") \
#     .start()


    
# Monitor query performance safely
# try:
#     print("Streaming query started. Press Ctrl+C to stop.")
#     while household_query.isActive :
#         progress = household_query.lastProgress
#         if progress:
#             # Safely access nested dictionary values
#             batch_id = progress.get('batchId', 'unknown')
#             num_rows = progress.get('numInputRows', 0)
#             trigger_info = progress.get('triggerExecution', {})
#             duration_ms = trigger_info.get('durationMs', 0) if trigger_info else 0
            
#             print(f"Progress: batch {batch_id}, records: {num_rows}, batch time: {duration_ms} ms")
#         time.sleep(5)
# except KeyboardInterrupt:
#     print("Stopping queries...")
#     household_query.stop()
    # console_household_query.stop()

# Wait for the queries to terminate
try:
    household_query.awaitTermination()
    # console_household_query.awaitTermination()
except Exception as e:
    print(f"Error during query execution: {e}")