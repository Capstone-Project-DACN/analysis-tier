import os
import boto3
from botocore.client import Config
from monitoring import log_message, stats

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
    """Generic function to write data to MinIO using CSV format
    
    Parameters:
    -----------
    batch_df : DataFrame
        The batch DataFrame to process
    batch_id : str
        Identifier for this batch
    data_type : str
        Type of data being processed (household or ward)
    spark : SparkSession
        The Spark session
    s3_bucket : str
        The S3 bucket name to write to
    """
    import time
    
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
    
    # Loop through each row in the batch
    for row in batch_df.collect():
        # Extract common values
        bucket_name = row["bucket_name"]
        formatted_ts = row["formatted_timestamp"].replace(":", "-").replace(" ", "-")
        
        # Create file path for this record
        file_path = f"s3a://{s3_bucket}/{bucket_name}/{formatted_ts}"
        
        try:
            # Create a record dataframe based on the data type
            if data_type == 'household':
                # Extract household-specific values
                household_id = row["household_id"]
                electricity_usage = row["electricity_usage_kwh"]
                voltage = row["voltage"]
                current = row["current"]
                
                # Create household-specific record dataframe
                data = [(household_id, electricity_usage, voltage, current, formatted_ts)]
                columns = ["household_id", "electricity_usage_kwh", "voltage", "current", 
                           "formatted_timestamp"]
            
            else:  # ward
                # Extract ward-specific values
                device_id = row["device_id"]
                type_val = row["type"]
                total_usage = row["total_electricity_usage_kwh"]
                
                # Create ward-specific record dataframe
                data = [(device_id, type_val, total_usage, formatted_ts)]
                columns = ["device_id", "type", "total_electricity_usage_kwh", "formatted_timestamp"]
            
            # Create the dataframe
            record_df = spark.createDataFrame(data, columns)
            
            # Write this record
            record_df.write \
                .mode("append") \
                .option("header", "true") \
                .csv(file_path)
            
            success_count += 1
        
        except Exception as e:
            id_value = household_id if data_type == 'household' else device_id
            log_message(f"Error writing {data_type} record {id_value} to {file_path}: {e}", "ERROR")
            stats['errors'][f'{data_type}_write_error'] += 1
            error_count += 1
    
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