#minio_handler.py
import os
import boto3
from botocore.client import Config
from monitoring import log_message, stats, start_batch_metrics, finish_batch_metrics
import time
from pyspark.sql.functions import col, lit
import json
from io import StringIO

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

def write_to_path(df, path, mode="overwrite"):
    try:
        df.write.mode(mode).json(path)
        return True
    except Exception as e:
        log_message(f"Error writing to {path}: {e}", "ERROR")
        return False

def write_single_json_file(df, path, bucket_name):
    """Write dataframe as a single JSON file instead of a folder
    
    Parameters:
    -----------
    df : DataFrame
        The DataFrame to write (should be very small, typically 1 row)
    path : str
        Full S3 path including s3a:// prefix
    bucket_name : str
        S3 bucket name
        
    Returns:
    --------
    bool : Success status
    """
    try:
        s3_client = get_s3_client()
        
        # Import required libraries
        import json
        from datetime import datetime, date
        
        # Custom JSON encoder to handle datetime objects
        class DateTimeEncoder(json.JSONEncoder):
            def default(self, obj):
                if isinstance(obj, (datetime, date)):
                    return obj.isoformat()
                return super(DateTimeEncoder, self).default(obj)
        
        # Convert DataFrame to a JSON string with custom encoder
        if df.count() == 1:
            # Convert single row to Python dictionary
            row = df.first()
            row_dict = {}
            
            # Process each field, handling datetime conversion explicitly
            for field in df.schema.fields:
                field_name = field.name
                field_value = row[field_name]
                
                # Handle different types that need conversion
                if isinstance(field_value, (datetime, date)):
                    row_dict[field_name] = field_value.isoformat()
                else:
                    row_dict[field_name] = field_value
            
            # Convert dictionary to JSON
            json_string = json.dumps(row_dict, cls=DateTimeEncoder)
        else:
            # For multiple rows, collect as JSON strings and join
            # This uses Spark's built-in toJSON which handles datetime conversion
            json_rows = df.toJSON().collect()
            json_string = "[" + ",".join(json_rows) + "]"
        
        # Extract the key from the path
        if path.startswith(f"s3a://{bucket_name}/"):
            key = path[len(f"s3a://{bucket_name}/"):]
        else:
            raise ValueError(f"Path {path} doesn't start with expected prefix s3a://{bucket_name}/")
        
        # Use the S3 client to put the object
        s3_client.put_object(
            Bucket=bucket_name,
            Key=key,
            Body=json_string,
            ContentType='application/json'
        )
        
        log_message(f"Successfully wrote single JSON file to {path}")
        return True
    except Exception as e:
        log_message(f"Error writing single JSON file to {path}: {e}", "ERROR")
        return False

def write_data_at_all_levels(device_df, device_id, data_type, s3_bucket, date_val, hour_val, minute_val):
    success_count = 0
    error_count = 0
    
    # Get the latest record for this device, date, hour, minute combination
    latest_record = device_df.orderBy(col("formatted_timestamp").desc()).limit(1)
    
    # Base path for this device
    base_path = f"s3a://{s3_bucket}/{device_id}"
    
    # Format paths (only JSON format)
    minute_path = f"{base_path}/{date_val}/{hour_val}/{minute_val}.json"
    hour_path = f"{base_path}/{date_val}/{hour_val}.json"
    day_path = f"{base_path}/{date_val}.json"
    
    # Write at minute level
    if write_single_json_file(latest_record, minute_path, s3_bucket):
        success_count += 1
    else:
        error_count += 1
    
    # Write at hour level
    if write_single_json_file(latest_record, hour_path, s3_bucket):
        success_count += 1
    else:
        error_count += 1
    
    # Write at day level
    if write_single_json_file(latest_record, day_path, s3_bucket):
        success_count += 1
    else:
        error_count += 1
    
    return success_count, error_count

def process_device_data(device_df, device_id, data_type, s3_bucket):
    success_count = 0
    error_count = 0
    
    # Get unique dates for this device
    date_groups = device_df.select("date_part").distinct()
    
    for date_row in date_groups.collect():
        date_val = date_row["date_part"]
        
        # Filter records for this date
        date_records = device_df.filter(col("date_part") == lit(date_val))
        
        # Get unique hours for this date
        hour_groups = date_records.select("hour").distinct()
        
        for hour_row in hour_groups.collect():
            hour_val = hour_row["hour"]
            
            # Filter records for this hour
            hour_records = date_records.filter(col("hour") == lit(hour_val))
            
            # Get unique minutes for this hour
            minute_groups = hour_records.select("minute").distinct()
            
            for minute_row in minute_groups.collect():
                minute_val = minute_row["minute"]
                
                # Filter records for this minute
                minute_records = hour_records.filter(col("minute") == lit(minute_val))
                
                # Write data at all levels (minute, hour, day)
                s_count, e_count = write_data_at_all_levels(
                    minute_records, device_id, data_type, s3_bucket,
                    date_val, hour_val, minute_val
                )
                
                success_count += s_count
                error_count += e_count
    
    return success_count, error_count

def get_executor_metrics(spark):
    """Get executor metrics from SparkContext"""
    try:
        metrics = {}
        
        # Get executor information
        executor_info = spark.sparkContext.statusTracker().getExecutorInfos()
        if executor_info:
            total_cores = sum(exec.totalCores for exec in executor_info)
            active_tasks = sum(len(exec.activeTasks) for exec in executor_info)
            
            metrics['executor_count'] = len(executor_info)
            metrics['total_cores'] = total_cores
            metrics['active_tasks'] = active_tasks

        return metrics
    except Exception as e:
        log_message(f"Error getting executor metrics: {e}", "ERROR")
        return {}

def write_data_to_minio(batch_df, batch_id, data_type, spark, s3_bucket):
    """Process a batch of data and write to MinIO storage
    
    Parameters:
    -----------
    batch_df : DataFrame
        The DataFrame containing the batch of data
    batch_id : int
        The batch identifier
    data_type : str
        The type of data ('household' or 'ward')
    spark : SparkSession
        The active SparkSession
    s3_bucket : str
        The MinIO bucket name
    """
    # Start tracking batch metrics
    start_batch_metrics(data_type, batch_id)
    
    # Record the starting time
    batch_start_time = time.time()
    batch_count = batch_df.count()
    
    # Update monitoring stats
    stats['last_10_batch_sizes'].append(batch_count)
    
    if data_type == 'household':
        stats['household_batches_processed'] += 1
        stats['household_records_processed'] += batch_count
    else:  # area
        stats['ward_batches_processed'] += 1
        stats['ward_records_processed'] += batch_count
        
    stats['total_records_processed'] += batch_count
    
    if batch_count == 0:
        log_message(f"{data_type.capitalize()} Batch {batch_id}: No data to process")
        # Finish batch metrics tracking with zero records
        batch_time_ms = (time.time() - batch_start_time) * 1000
        finish_batch_metrics(data_type, batch_id, 0, batch_time_ms, {})
        return
        
    log_message(f"{data_type.capitalize()} Batch {batch_id}: Processing {batch_count} records")
    
    success_count = 0
    error_count = 0
    
    # Group by device_id
    device_groups = batch_df.select("device_id").distinct()
    
    for device_row in device_groups.collect():
        device_id = device_row["device_id"]
        
        # Filter for this device
        device_records = batch_df.filter(col("device_id") == lit(device_id))
        
        # Process this device's data
        s_count, e_count = process_device_data(device_records, device_id, data_type, s3_bucket)
        success_count += s_count
        error_count += e_count
    
    # Track performance metrics
    batch_end_time = time.time()
    elapsed = batch_end_time - batch_start_time
    batch_time_ms = elapsed * 1000
    
    if data_type == 'household':
        stats['household_processing_time_ms'] += batch_time_ms
    else:  # area
        stats['ward_processing_time_ms'] += batch_time_ms
        
    stats['last_10_processing_times'].append(batch_time_ms)
    
    # Get executor metrics
    executor_metrics = get_executor_metrics(spark)
    
    # Finish batch metrics tracking
    finish_batch_metrics(
        batch_type=data_type,
        batch_id=batch_id,
        records_count=batch_count,
        processing_time_ms=batch_time_ms,
        executor_metrics=executor_metrics
    )
    
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