import time
import os
import threading
import datetime
import json
from collections import deque, defaultdict

# Stats tracking
stats = {
    'start_time': time.time(),
    'total_records_processed': 0,
    'household_records_processed': 0,
    'ward_records_processed': 0,
    'household_batches_processed': 0,
    'ward_batches_processed': 0,
    'household_processing_time_ms': 0,
    'ward_processing_time_ms': 0,
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
    """Thread to monitor overall job statistics"""
    while True:
        try:
            # Calculate runtime statistics
            runtime = time.time() - stats['start_time']
            hours, remainder = divmod(runtime, 3600)
            minutes, seconds = divmod(remainder, 60)
            
            # Pre-calculate all values
            total_records = stats['total_records_processed']
            household_records = stats['household_records_processed']
            ward_records = stats['ward_records_processed']
            
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
            Household Records: {household_records}
            Ward Records: {ward_records}
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

def monitor_kafka_lag(household_query, ward_query):
    """Thread to monitor Kafka consumer lag"""
    while True:
        try:
            # Monitor streaming query progress
            query_statuses = {}
            
            # Check household query status
            if household_query and household_query.isActive:
                progress = household_query.lastProgress
                if progress:
                    query_statuses['household'] = {
                        'inputRows': progress.get('numInputRows', 0),
                        'inputRowsPerSecond': progress.get('inputRowsPerSecond', 0),
                        'processedRowsPerSecond': progress.get('processedRowsPerSecond', 0),
                        'durationMs': progress.get('triggerExecution', {}).get('durationMs', 0)
                    }
            
            # Check ward query status
            if ward_query and ward_query.isActive:
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
            
            time.sleep(60)  # Check every 60 seconds
        except Exception as e:
            log_message(f"Error in Kafka monitoring: {e}", "ERROR")
            time.sleep(5)

def monitor_minio_status(s3_client, buckets):
    """Thread to monitor MinIO health and bucket stats"""
    while True:
        try:
            # Check MinIO health
            try:
                response = s3_client.list_buckets()
                bucket_count = len(response['Buckets'])
                log_message(f"MinIO Health Check: OK - {bucket_count} buckets available")
                
                # Get storage statistics for our buckets
                for bucket in buckets:
                    try:
                        # This is a simple way to check if the bucket exists and is accessible
                        response = s3_client.list_objects_v2(Bucket=bucket, MaxKeys=1)
                        prefix_stats = {}
                        
                        # Get stats on some key prefixes 
                        sample_prefixes = ['hcmc', 'hanoi', 'danang']
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

def start_monitoring(household_query=None, ward_query=None, s3_client=None, buckets=None):
    """Start all monitoring threads"""
    # Main monitoring thread
    monitor_thread = threading.Thread(target=monitor_streaming_job, daemon=True)
    monitor_thread.start()
    
    # Start Kafka monitoring if queries provided
    if household_query or ward_query:
        kafka_thread = threading.Thread(
            target=monitor_kafka_lag, 
            args=(household_query, ward_query), 
            daemon=True
        )
        kafka_thread.start()
    
    # Start MinIO monitoring if client provided
    if s3_client and buckets:
        minio_thread = threading.Thread(
            target=monitor_minio_status, 
            args=(s3_client, buckets), 
            daemon=True
        )
        minio_thread.start()
    
    return log_message("All monitoring threads started")