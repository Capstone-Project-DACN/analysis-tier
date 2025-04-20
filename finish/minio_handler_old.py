import os
import boto3
from botocore.client import Config
from monitoring import log_message, stats
import time
from pyspark.sql.functions import col, lit
# MinIO connection parameters
MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "myminioadmin")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "myminioadmin")

def get_s3_client():
    """Create and return a MinIO S3 client"""
    return boto3.client(
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

def initialize_buckets(s3_client, buckets):
    """Initialize specified MinIO buckets"""
    for bucket in buckets:
        try:
            s3_client.head_bucket(Bucket=bucket)
            log_message(f"Bucket '{bucket}' already exists")
        except:
            try:
                s3_client.create_bucket(Bucket=bucket)
                log_message(f"Created bucket '{bucket}'")
            except Exception as e:
                log_message(f"Error creating bucket '{bucket}': {e}", "ERROR")
                stats['errors']['bucket_creation'] += 1
    
    return True

def write_data_to_minio(batch_df, batch_id, data_type, spark, s3_bucket):

    
    batch_start_time = time.time()
    batch_count = batch_df.count()
    
    # Update monitoring stats
    stats['last_10_batch_sizes'].append(batch_count)
    
    if data_type == 'household':
        stats['household_batches_processed'] += 1
        stats['household_records_processed'] += batch_count
    else:  # ward
        stats['ward_batches_processed'] += 1
        stats['ward_records_processed'] += batch_count
        
    stats['total_records_processed'] += batch_count
    
    if batch_count == 0:
        log_message(f"{data_type.capitalize()} Batch {batch_id}: No data to process")
        return
        
    log_message(f"{data_type.capitalize()} Batch {batch_id}: Processing {batch_count} records")
    
    success_count = 0
    error_count = 0
    
    # Group records by bucket_name to minimize the number of write operations
    # This is a key optimization without changing your storage structure
    bucket_groups = batch_df.groupBy("bucket_name").agg({})
    bucket_names = [row["bucket_name"] for row in bucket_groups.collect()]
    
    for bucket_name in bucket_names:
        # Filter records for this bucket
        bucket_records = batch_df.filter(col("bucket_name") == lit(bucket_name))
        
        # Get all the formatted timestamps for this bucket
        timestamp_groups = bucket_records.select("formatted_timestamp").distinct()
        formatted_timestamps = [row["formatted_timestamp"] for row in timestamp_groups.collect()]
        
        for formatted_ts in formatted_timestamps:
            # Select all records matching this bucket_name and timestamp combination
            # This allows us to write multiple records with the same path in a single write operation
            records = bucket_records.filter(col("formatted_timestamp") == lit(formatted_ts))
            record_count = records.count()
            
            if record_count == 0:
                continue
                
            # Create file path for these records (same as your original)
            date_part = formatted_ts.split(" ")[0] 
            file_path = f"s3a://{s3_bucket}/{bucket_name}/{date_part}"
            
            try:
                # For household data
                if data_type == 'household':
                    # Create a simplified DataFrame with only the needed columns
                    output_records = records.select(
                        "device_id", 
                        "electricity_usage_kwh", 
                        "voltage", 
                        "current", 
                        "formatted_timestamp"
                    )
                
                # For ward data
                else:  # ward
                    output_records = records.select(
                        "device_id", 
                        "total_electricity_usage_kwh", 
                        "formatted_timestamp"
                    )
                
                # Write all records with the same path in a single operation
                # This is much more efficient than writing one by one
                output_records.write \
                    .mode("append") \
                    .option("header", "true") \
                    .json(file_path)
                
                success_count += record_count
                
            except Exception as e:
                error_msg = f"Error writing {data_type} records to {file_path}: {e}"
                log_message(error_msg, "ERROR")
                stats['errors'][f'{data_type}_write_error'] += 1
                error_count += record_count
    
    # Track performance metrics
    batch_end_time = time.time()
    elapsed = batch_end_time - batch_start_time
    batch_time_ms = elapsed * 1000
    
    if data_type == 'household':
        stats['household_processing_time_ms'] += batch_time_ms
    else:  # ward
        stats['ward_processing_time_ms'] += batch_time_ms
        
    stats['last_10_processing_times'].append(batch_time_ms)
    
    # Log performance metrics
    records_per_second = batch_count / elapsed if elapsed > 0 else 0
    log_message(f"{data_type.capitalize()} Batch {batch_id}: Completed in {elapsed:.2f} seconds ({records_per_second:.2f} records/sec)")
    log_message(f"{data_type.capitalize()} Batch {batch_id}: Success: {success_count}, Errors: {error_count}")
    
def get_spark_minio_config(jars_path):
    """Returns Spark configuration dictionary for MinIO"""
    return {
        "spark.hadoop.fs.s3a.endpoint": MINIO_ENDPOINT,
        "spark.hadoop.fs.s3a.access.key": MINIO_ACCESS_KEY,
        "spark.hadoop.fs.s3a.secret.key": MINIO_SECRET_KEY,
        "spark.hadoop.fs.s3a.path.style.access": "true",
        "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        "spark.jars": jars_path,
        "spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version": "2",
        "spark.hadoop.fs.s3a.fast.upload": "true",
        "spark.hadoop.fs.s3a.fast.upload.buffer": "bytebuffer",
        "spark.hadoop.fs.s3a.multipart.size": "5242880",
        "spark.hadoop.fs.s3a.threads.max": "20",
        "spark.hadoop.fs.s3a.connection.maximum": "30",
        "spark.hadoop.fs.s3a.connection.establish.timeout": "5000",
        "spark.hadoop.fs.s3a.connection.timeout": "10000",
        "spark.hadoop.fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
    }