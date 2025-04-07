from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, min, max, avg, year, month, sum, date_format, concat, lit, regexp_replace, lower
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType, TimestampType
import time
import os
import boto3
from botocore.client import Config
import threading
import datetime
from collections import deque, defaultdict

# Get absolute path to the jars directory
jars_dir = os.path.abspath("./jars")
jars = ",".join([
    f"{jars_dir}/hadoop-aws-3.3.1.jar",
    f"{jars_dir}/aws-java-sdk-bundle-1.11.901.jar",
    f"{jars_dir}/wildfly-openssl-1.0.7.Final.jar"
])

# MinIO connection parameters
MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "myminioadmin")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "myminioadmin")

# Create a Spark session with optimized MinIO configuration
spark = SparkSession.builder \
    .appName("Kafka to MinIO Household Data") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0") \
    .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
    .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
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

# Set error-only logging for Spark system logs
spark.sparkContext.setLogLevel("ERROR")

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

# ============= MONITORING SETUP =============
# Stats tracking
stats = {
    'start_time': time.time(),
    'total_records_processed': 0,
    'daily_records_processed': 0,
    'monthly_records_processed': 0,
    'daily_batches_processed': 0,
    'monthly_batches_processed': 0,
    'daily_processing_time_ms': 0,
    'monthly_processing_time_ms': 0,
    'errors': defaultdict(int),
    'last_10_batch_sizes': deque(maxlen=10),
    'last_10_processing_times': deque(maxlen=10)
}

# Create monitoring log file
log_dir = "./logs"
os.makedirs(log_dir, exist_ok=True)
log_file = f"{log_dir}/spark_streaming_monitor_{int(time.time())}.log"

def log_message(message, level="INFO"):
    """Log a message to the log file and print to console"""
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_line = f"[{timestamp}] [{level}] {message}"
    
    # Print to console
    print(log_line)
    
    # Write to log file
    with open(log_file, "a") as f:
        f.write(log_line + "\n")

def monitor_streaming_job():
    """Extremely simplified monitoring thread"""
    while True:
        try:
            # Calculate runtime statistics
            runtime = time.time() - stats['start_time']
            hours, remainder = divmod(runtime, 3600)
            minutes, seconds = divmod(remainder, 60)
            
            # Pre-calculate all values
            total_records = stats['total_records_processed']
            daily_records = stats['daily_records_processed']
            monthly_records = stats['monthly_records_processed']
            
            # Pre-compute error count outside the f-string
            error_count = 0
            for err_count in stats['errors'].values():
                error_count += err_count
            
            # Calculate throughput
            records_per_second = total_records / (runtime if runtime > 0 else 1)
            
            # Construct message with only simple variables
            status = f"""
            ============= STREAMING JOB STATUS =============
            Runtime: {int(hours)}h {int(minutes)}m {int(seconds)}s
            Total Records: {total_records}
            Daily Records: {daily_records}
            Monthly Records: {monthly_records}
            Throughput: {records_per_second:.2f} records/second
            Error Count: {error_count}
            =================================================
            """
            # Use direct printing
            print(status)
            
            # No complex operations in file writing
            with open(f"{log_dir}/stats_{int(time.time())}.json", "w") as f:
                f.write(f'{{"timestamp": "{datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")}", '
                       f'"runtime": {int(runtime)}, '
                       f'"total_records": {total_records}, '
                       f'"error_count": {error_count}}}')
            
            time.sleep(30)
        except Exception as e:
            print(f"Error in monitoring thread: {e}")
            time.sleep(5)      
# Start monitoring thread
monitor_thread = threading.Thread(target=monitor_streaming_job, daemon=True)
monitor_thread.start()


# Define Kafka source parameters
KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "localhost:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "household_data")
KAFKA_MAX_OFFSET = os.environ.get("KAFKA_MAX_OFFSET","500")

# Read data from Kafka with increased batch size
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("subscribe", KAFKA_TOPIC) \
    .option("startingOffsets", "earliest") \
    .option("maxOffsetsPerTrigger", KAFKA_MAX_OFFSET) \
    .option("failOnDataLoss", "false") \
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

s3_client = boto3.client(
    's3',
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=MINIO_ACCESS_KEY,
    aws_secret_access_key=MINIO_SECRET_KEY,
    config=Config(
        signature_version='s3v4',
        max_pool_connections=30,
        retries={'max_attempts': 3}
    ),
    region_name='us-east-1'
)

# Create a 'monthly', 'daily' bucket at startup
monthly_bucket = "monthly"
daily_bucket = "daily"

try:
    s3_client.head_bucket(Bucket=monthly_bucket)
    log_message(f"Bucket '{monthly_bucket}' already exists")
    s3_client.head_bucket(Bucket=daily_bucket)
    log_message(f"Bucket '{daily_bucket}' already exists")
except:
    try:
        s3_client.create_bucket(Bucket=monthly_bucket)
        log_message(f"Created bucket '{monthly_bucket}'")
        s3_client.create_bucket(Bucket=daily_bucket)
        log_message(f"Created bucket '{daily_bucket}'")
    except Exception as e:
        log_message(f"Error creating bucket: {e}", "ERROR")
        stats['errors']['bucket_creation'] += 1

# Function to write aggregated household data to MinIO
def write_monthly_data_to_minio(batch_df, batch_id):
    batch_start_time = time.time()
    batch_count = batch_df.count()
    
    # Update monitoring stats
    stats['last_10_batch_sizes'].append(batch_count)
    stats['monthly_batches_processed'] += 1
    stats['monthly_records_processed'] += batch_count
    stats['total_records_processed'] += batch_count
    
    log_message(f"Monthly Batch {batch_id}: Processing {batch_count} records")
    
    if batch_count == 0:
        log_message(f"Monthly Batch {batch_id}: No data to process")
        return
    
    success_count = 0
    error_count = 0
    # Get unique bucket names 
    bucket_names = [row.bucket_name for row in batch_df.select("bucket_name").distinct().collect()]
    
    for bucket_name in bucket_names:
        # Filter data for this bucket
        bucket_data = batch_df.filter(col("bucket_name") == bucket_name)
        bucket_record_count = bucket_data.count()
        
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
        file_path = f"s3a://{monthly_bucket}/{bucket_name}/{year}/{month}"
        
        try:
            # Using CSV format instead of text for better reliability
            output_data.write \
                .mode("overwrite") \
                .option("header", "true") \
                .csv(file_path)
                
            log_message(f"Monthly Batch {batch_id}: Successfully wrote {bucket_record_count} records to {file_path}")
            success_count += bucket_record_count
        except Exception as e:
            log_message(f"Monthly Batch {batch_id}: Error writing to {file_path}: {e}", "ERROR")
            stats['errors']['monthly_write_error'] += 1
            error_count += bucket_record_count
            
            # Try local fallback
            try:
                local_path = f"/tmp/household_data/{bucket_name}/{year}/{month}"
                output_data.write \
                    .mode("overwrite") \
                    .option("header", "true") \
                    .csv(local_path)
                log_message(f"Monthly Batch {batch_id}: Wrote {bucket_record_count} records to local path: {local_path}")
                success_count += bucket_record_count  # Count as success if local write works
                error_count -= bucket_record_count
            except Exception as local_err:
                log_message(f"Monthly Batch {batch_id}: Error writing to local path: {local_err}", "ERROR")
                stats['errors']['monthly_local_write_error'] += 1
    
    batch_end_time = time.time()
    elapsed = batch_end_time - batch_start_time
    batch_time_ms = elapsed * 1000
    stats['monthly_processing_time_ms'] += batch_time_ms
    stats['last_10_processing_times'].append(batch_time_ms)
    
    records_per_second = batch_count / elapsed if elapsed > 0 else 0
    log_message(f"Monthly Batch {batch_id}: Completed in {elapsed:.2f} seconds ({records_per_second:.2f} records/sec)")
    log_message(f"Monthly Batch {batch_id}: Success: {success_count}, Errors: {error_count}")
    
def write_daily_data_to_minio(batch_df, batch_id):
    """Write raw household data to MinIO using CSV format with timestamp in the filename"""
    batch_start_time = time.time()
    batch_count = batch_df.count()
    
    # Update monitoring stats
    stats['last_10_batch_sizes'].append(batch_count)
    stats['daily_batches_processed'] += 1
    stats['daily_records_processed'] += batch_count
    stats['total_records_processed'] += batch_count
    
    if batch_count == 0:
        log_message(f"Daily Batch {batch_id}: No data to process")
        return
        
    log_message(f"Daily Batch {batch_id}: Processing {batch_count} records")
    
    success_count = 0
    error_count = 0
    
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
        file_path = f"s3a://{daily_bucket}/{bucket_name}/{formatted_ts}"
        
        # Create a new small dataframe with just this record
        data = [(household_id, electricity_usage, voltage, current, formatted_ts)]
        columns = ["household_id", "electricity_usage_kwh", "voltage", "current", 
                   "formatted_timestamp"]
        record_df = spark.createDataFrame(data, columns)
        
        try:
            # Write this record
            record_df.write \
                .mode("append") \
                .option("header", "true") \
                .csv(file_path)
            success_count += 1
        except Exception as e:
            log_message(f"Error writing record {household_id} to {file_path}: {e}", "ERROR")
            stats['errors']['daily_write_error'] += 1
            error_count += 1
    
    batch_end_time = time.time()
    elapsed = batch_end_time - batch_start_time
    batch_time_ms = elapsed * 1000
    stats['daily_processing_time_ms'] += batch_time_ms
    stats['last_10_processing_times'].append(batch_time_ms)
    
    records_per_second = batch_count / elapsed if elapsed > 0 else 0
    log_message(f"Daily Batch {batch_id}: Completed in {elapsed:.2f} seconds ({records_per_second:.2f} records/sec)")
    log_message(f"Daily Batch {batch_id}: Success: {success_count}, Errors: {error_count}")

# Monitor Kafka consumer health
def monitor_kafka_lag():
    """Thread to monitor Kafka consumer lag"""
    import subprocess
    import json
    
    while True:
        try:
            # This would normally call kafka-consumer-groups.sh to get consumer lag
            # Since we don't have direct access, we'll simulate it with monitoring
            query_statuses = {}
            
            # Check household query status
            if 'household_query' in globals() and household_query.isActive:
                progress = household_query.lastProgress
                if progress:
                    query_statuses['household'] = {
                        'inputRows': progress.get('numInputRows', 0),
                        'inputRowsPerSecond': progress.get('inputRowsPerSecond', 0),
                        'processedRowsPerSecond': progress.get('processedRowsPerSecond', 0),
                        'durationMs': progress.get('triggerExecution', {}).get('durationMs', 0)
                    }
            
            # Check ward query status
            if 'ward_query' in globals() and ward_query.isActive:
                progress = ward_query.lastProgress
                if progress:
                    query_statuses['ward'] = {
                        'inputRows': progress.get('numInputRows', 0),
                        'inputRowsPerSecond': progress.get('inputRowsPerSecond', 0),
                        'processedRowsPerSecond': progress.get('processedRowsPerSecond', 0),
                        'durationMs': progress.get('triggerExecution', {}).get('durationMs', 0)
                    }
                    
            if query_statuses:
                log_message(f"Kafka consumer stats: {json.dumps(query_statuses)}")
            
            time.sleep(15)  # Check every 15 seconds
        except Exception as e:
            log_message(f"Error in Kafka monitoring: {e}", "ERROR")
            time.sleep(5)

# Start Kafka monitoring thread
kafka_monitor_thread = threading.Thread(target=monitor_kafka_lag, daemon=True)
kafka_monitor_thread.start()

# Function to monitor MinIO status
def monitor_minio_status():
    """Thread to monitor MinIO health and bucket stats"""
    while True:
        try:
            # Check MinIO health
            try:
                response = s3_client.list_buckets()
                bucket_count = len(response['Buckets'])
                log_message(f"MinIO Health Check: OK - {bucket_count} buckets available")
                
                # Get storage statistics for our buckets
                for bucket in [monthly_bucket, daily_bucket]:
                    try:
                        # This is a simple way to check if the bucket exists and is accessible
                        response = s3_client.list_objects_v2(Bucket=bucket, MaxKeys=1)
                        prefix_stats = {}
                        
                        # Get stats on some key prefixes to see data distribution
                        sample_prefixes = ['alaska', 'texas', 'california']
                        for prefix in sample_prefixes:
                            try:
                                prefix_objects = s3_client.list_objects_v2(
                                    Bucket=bucket, 
                                    Prefix=prefix,
                                    MaxKeys=1000
                                )
                                count = prefix_objects.get('KeyCount', 0)
                                prefix_stats[prefix] = count
                            except Exception:
                                prefix_stats[prefix] = "error"
                                
                        log_message(f"Bucket {bucket} statistics: {prefix_stats}")
                    except Exception as e:
                        log_message(f"Error checking bucket {bucket}: {e}", "ERROR")
                        stats['errors']['minio_bucket_check'] += 1
                
            except Exception as e:
                log_message(f"MinIO Health Check: FAILED - {e}", "ERROR")
                stats['errors']['minio_health_check'] += 1
            
            time.sleep(60)  # Check every minute
        except Exception as e:
            log_message(f"Error in MinIO monitoring: {e}", "ERROR")
            time.sleep(10)

# Start MinIO monitoring thread
minio_monitor_thread = threading.Thread(target=monitor_minio_status, daemon=True)
minio_monitor_thread.start()

log_message("Starting streaming queries...")

# Start the streaming query with optimized settings
ward_query = ward_df.writeStream \
    .queryName("OptimizedHouseholdAggregation") \
    .outputMode("complete") \
    .option("checkpointLocation", "/tmp/checkpoint/ward_data") \
    .foreachBatch(write_monthly_data_to_minio) \
    .trigger(processingTime="10 seconds") \
    .start()

household_query = household_df.writeStream \
    .outputMode("append") \
    .option("checkpointLocation", "/tmp/checkpoint/household_data") \
    .foreachBatch(write_daily_data_to_minio) \
    .trigger(processingTime="10 seconds") \
    .start()

log_message("Streaming queries started successfully")

# Register cleanup handler
import atexit

def cleanup():
    """Cleanup function to be called on exit"""
    log_message("Shutting down streaming application...")
    
    # Stop all queries
    if 'ward_query' in globals():
        try:
            ward_query.stop()
            log_message("Stopped ward query")
        except:
            pass
    
    if 'household_query' in globals():
        try:
            household_query.stop()
            log_message("Stopped household query")
        except:
            pass
    
    # Final statistics
    runtime = time.time() - stats['start_time']
    hours, remainder = divmod(runtime, 3600)
    minutes, seconds = divmod(remainder, 60)
    
    log_message(f"""
=== FINAL STATISTICS ===
Total Runtime: {int(hours)}h {int(minutes)}m {int(seconds)}s
Total Records Processed: {stats['total_records_processed']}
Daily Records: {stats['daily_records_processed']}
Monthly Records: {stats['monthly_records_processed']}
Error Count: {sum(stats['errors'].values())}
Error Details: {dict(stats['errors'])}
========================
""", "INFO")

atexit.register(cleanup)

# Wait for query termination
try:
    log_message("Waiting for query termination...")
    ward_query.awaitTermination()
    household_query.awaitTermination()
except KeyboardInterrupt:
    log_message("Received keyboard interrupt, shutting down...", "WARNING")
    ward_query.stop()
    household_query.stop()
except Exception as e:
    log_message(f"Error during query execution: {e}", "ERROR")
    stats['errors']['query_execution'] += 1
    
    # Try to stop queries gracefully
    try:
        ward_query.stop()
    except:
        pass
        
    try:
        household_query.stop()
    except:
        pass