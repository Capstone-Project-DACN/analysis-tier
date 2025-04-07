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

# Create a Spark session with optimized MinIO configuration
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

s3_client = boto3.client(
    's3',
    endpoint_url=endpoint_url,
    aws_access_key_id=access_key,
    aws_secret_access_key=secret_key,
    config=Config(
        signature_version='s3v4',
        max_pool_connections=30,
        retries={'max_attempts': 3}
    ),
    region_name='us-east-1'
)

# Create a 'monthly' bucket at startup
main_bucket = "monthly"
try:
    s3_client.head_bucket(Bucket=main_bucket)
    print(f"Main bucket '{main_bucket}' already exists")
except:
    try:
        s3_client.create_bucket(Bucket=main_bucket)
        print(f"Created main bucket '{main_bucket}'")
    except Exception as e:
        print(f"Error creating bucket: {e}")

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

# Read data from Kafka with increased batch size
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
    start_time = time.time()
    batch_count = batch_df.count()
    print(f"Batch {batch_id}: Processing {batch_count} records")
    
    if batch_count == 0:
        print(f"Batch {batch_id}: No data to process")
        return
    
    # Get unique bucket names 
    bucket_names = [row.bucket_name for row in batch_df.select("bucket_name").distinct().collect()]
    
    for bucket_name in bucket_names:
        # Filter data for this bucket
        bucket_data = batch_df.filter(col("bucket_name") == bucket_name)
        
        # Get the year and month from the first row for the path structure
        first_row = bucket_data.first()
        year = str(first_row["year"])
        month = str(first_row["month"])
        
        # Select columns for output, similar to original
        output_data = bucket_data.select(
            "min_electricity_usage_kwh",
            "max_electricity_usage_kwh", 
            "avg_electricity_usage_kwh",
            "year",
            "month"
        )
        
        # Create path for the data - using original structure
        file_path = f"s3a://{main_bucket}/{bucket_name}/{year}/{month}"
        
        try:
            # Using CSV format instead of text for better reliability
            output_data.write \
                .mode("overwrite") \
                .option("header", "true") \
                .csv(file_path)
                
            print(f"Batch {batch_id}: Successfully wrote data to {file_path}")
        except Exception as e:
            print(f"Batch {batch_id}: Error writing to {file_path}: {e}")
            
            # Try local fallback
            try:
                local_path = f"/tmp/household_data/{bucket_name}/{year}/{month}"
                output_data.write \
                    .mode("overwrite") \
                    .option("header", "true") \
                    .csv(local_path)
                print(f"Batch {batch_id}: Wrote data to local path: {local_path}")
            except Exception as local_err:
                print(f"Batch {batch_id}: Error writing to local path: {local_err}")
    
    elapsed = time.time() - start_time
    records_per_second = batch_count / elapsed if elapsed > 0 else 0
    print(f"Batch {batch_id}: Completed in {elapsed:.2f} seconds ({records_per_second:.2f} records/sec)")

# Start the streaming query with optimized settings
ward_query = ward_df.writeStream \
    .queryName("OptimizedHouseholdAggregation") \
    .outputMode("complete") \
    .option("checkpointLocation", "/tmp/checkpoint/ward_data") \
    .foreachBatch(write_aggregated_data_to_minio) \
    .trigger(processingTime="10 seconds") \
    .start()

# Monitor query performance safely
try:
    print("Streaming query started. Press Ctrl+C to stop.")
    while ward_query.isActive:
        progress = ward_query.lastProgress
        if progress:
            # Safely access nested dictionary values
            batch_id = progress.get('batchId', 'unknown')
            num_rows = progress.get('numInputRows', 0)
            trigger_info = progress.get('triggerExecution', {})
            duration_ms = trigger_info.get('durationMs', 0) if trigger_info else 0
            
            print(f"Progress: batch {batch_id}, records: {num_rows}, batch time: {duration_ms} ms")
        time.sleep(5)
except KeyboardInterrupt:
    print("Stopping query...")
    ward_query.stop()

# Wait for query termination
ward_query.awaitTermination()