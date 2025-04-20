#monitoring.py
import time
import os
import threading
import datetime
import json
import psutil
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
    'last_10_processing_times': deque(maxlen=10),
    'spark_metrics': {},  # For Spark UI metrics
    'spark_ui_url': None,  # Will be populated when Spark UI is available
    'batch_history': {     # New field for tracking batch metrics
        'household': deque(maxlen=50),  # Keep history of last 50 batches
        'ward': deque(maxlen=50)
    },
    'system_metrics': {    # New field for system resource metrics
        'cpu_percent': 0,
        'memory_usage_percent': 0,
        'memory_usage_mb': 0,
        'memory_available_mb': 0
    },
    # Track batch resource usage
    'active_batch': {
        'is_processing': False,
        'type': None,
        'batch_id': None,
        'start_time': 0,
        'start_cpu_times': None,
        'start_memory': 0,
        'start_cpu_percent': 0,
    }
}

# Process ID for resource tracking
CURRENT_PID = os.getpid()
PROCESS = psutil.Process(CURRENT_PID)

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

def collect_system_metrics():
    """Collect system metrics (CPU, memory)"""
    try:
        # Get CPU usage
        cpu_percent = psutil.cpu_percent(interval=0.1)
        
        # Get memory usage using multiple methods to ensure we get a value
        memory_info = {}
        
        # Method 1: Direct virtual_memory call
        try:
            memory = psutil.virtual_memory()
            memory_info = {
                'percent': memory.percent,
                'used_mb': memory.used / (1024 * 1024),
                'available_mb': memory.available / (1024 * 1024),
                'total_mb': memory.total / (1024 * 1024)
            }
        except Exception as e:
            log_message(f"Method 1 memory collection failed: {e}", "DEBUG")
        
        # Method 2: If first method failed, try process-specific memory
        if not memory_info:
            try:
                process_memory = PROCESS.memory_info()
                total_memory = os.sysconf('SC_PAGE_SIZE') * os.sysconf('SC_PHYS_PAGES')
                memory_info = {
                    'percent': (process_memory.rss / total_memory) * 100,
                    'used_mb': process_memory.rss / (1024 * 1024),
                    'available_mb': (total_memory - process_memory.rss) / (1024 * 1024),
                    'total_mb': total_memory / (1024 * 1024)
                }
            except Exception as e:
                log_message(f"Method 2 memory collection failed: {e}", "DEBUG")
        
        # Method 3: If all else fails, use Docker-specific approach
        if not memory_info:
            try:
                with open('/sys/fs/cgroup/memory/memory.usage_in_bytes', 'r') as f:
                    memory_usage = int(f.read().strip())
                
                with open('/sys/fs/cgroup/memory/memory.limit_in_bytes', 'r') as f:
                    memory_limit = int(f.read().strip())
                
                memory_info = {
                    'percent': (memory_usage / memory_limit) * 100,
                    'used_mb': memory_usage / (1024 * 1024),
                    'available_mb': (memory_limit - memory_usage) / (1024 * 1024),
                    'total_mb': memory_limit / (1024 * 1024)
                }
            except Exception as e:
                log_message(f"Method 3 memory collection failed: {e}", "DEBUG")
                # Provide fallback values if all methods fail
                memory_info = {
                    'percent': 0,
                    'used_mb': 0,
                    'available_mb': 0,
                    'total_mb': 1
                }
        
        # Update stats
        stats['system_metrics']['cpu_percent'] = cpu_percent
        stats['system_metrics']['memory_usage_percent'] = memory_info['percent']
        stats['system_metrics']['memory_usage_mb'] = memory_info['used_mb']
        stats['system_metrics']['memory_available_mb'] = memory_info['available_mb']
        
        return {
            'cpu_percent': cpu_percent,
            'memory_percent': memory_info['percent'],
            'memory_used_mb': memory_info['used_mb'],
            'memory_available_mb': memory_info['available_mb']
        }
    except Exception as e:
        log_message(f"Error collecting system metrics: {e}", "ERROR")
        stats['errors']['system_metrics'] += 1
        return {
            'cpu_percent': 0,
            'memory_percent': 0,
            'memory_used_mb': 0,
            'memory_available_mb': 0
        }

def start_batch_metrics(batch_type, batch_id):
    """Start tracking metrics for a batch"""
    try:
        # Collect initial metrics
        stats['active_batch']['is_processing'] = True
        stats['active_batch']['type'] = batch_type
        stats['active_batch']['batch_id'] = batch_id
        stats['active_batch']['start_time'] = time.time()
        
        # Get initial CPU times for the process
        stats['active_batch']['start_cpu_times'] = PROCESS.cpu_times()
        
        # Get initial memory usage
        stats['active_batch']['start_memory'] = PROCESS.memory_info().rss
        
        # Get initial CPU percentage (need to call twice for accurate measurement)
        PROCESS.cpu_percent(interval=None)  # First call to initialize
        time.sleep(0.1)  # Short sleep
        stats['active_batch']['start_cpu_percent'] = PROCESS.cpu_percent(interval=None)
        
        log_message(f"Started metrics collection for {batch_type} batch {batch_id}", "DEBUG")
    except Exception as e:
        log_message(f"Error starting batch metrics: {e}", "ERROR")
        stats['errors']['batch_metrics_start'] += 1

def finish_batch_metrics(batch_type, batch_id, records_count, processing_time_ms, executor_metrics=None):
    """Finish metrics collection for a batch and record the results"""
    if not stats['active_batch']['is_processing'] or stats['active_batch']['batch_id'] != batch_id:
        # This is not the batch we're tracking, use standard metrics
        record_batch_metrics(batch_type, batch_id, records_count, processing_time_ms, executor_metrics)
        return
    
    try:
        # Calculate CPU usage for this batch
        end_cpu_times = PROCESS.cpu_times()
        start_cpu_times = stats['active_batch']['start_cpu_times']
        
        # Calculate the CPU time difference
        cpu_user_time = end_cpu_times.user - start_cpu_times.user
        cpu_system_time = end_cpu_times.system - start_cpu_times.system
        cpu_total_time = cpu_user_time + cpu_system_time
        
        # Calculate the wall time difference
        wall_time = time.time() - stats['active_batch']['start_time']
        wall_time_ms = wall_time * 1000
        
        # Calculate CPU percentage (can be > 100% with multiple cores)
        if wall_time > 0:
            batch_cpu_percent = (cpu_total_time / wall_time) * 100
        else:
            batch_cpu_percent = 0
        
        # Get current CPU percentage and calculate difference
        end_cpu_percent = PROCESS.cpu_percent(interval=None)
        start_cpu_percent = stats['active_batch']['start_cpu_percent']
        cpu_percent_diff = end_cpu_percent - start_cpu_percent
        
        # Calculate memory usage difference
        end_memory = PROCESS.memory_info().rss
        start_memory = stats['active_batch']['start_memory']
        memory_diff_mb = (end_memory - start_memory) / (1024 * 1024)
        
        # Get overall system metrics at this point
        system_metrics = collect_system_metrics()
        
        # Create batch metrics record with batch-specific resource usage
        timestamp = datetime.datetime.now().isoformat()
        
        batch_metrics = {
            'batch_id': batch_id,
            'timestamp': timestamp,
            'records_count': records_count,
            'processing_time_ms': processing_time_ms,
            'processing_rate': (records_count / (processing_time_ms / 1000)) if processing_time_ms > 0 else 0,
            'system_metrics': system_metrics,
            'batch_resources': {
                'cpu_percent': batch_cpu_percent,
                'cpu_percent_diff': cpu_percent_diff,
                'memory_diff_mb': memory_diff_mb,
                'wall_time_ms': wall_time_ms,
                'cpu_time_user': cpu_user_time,
                'cpu_time_system': cpu_system_time
            }
        }
        
        # Add executor metrics if available
        if executor_metrics:
            batch_metrics['executor_metrics'] = executor_metrics
        
        # Reset active batch tracking
        stats['active_batch']['is_processing'] = False
        
        # Add to batch history
        stats['batch_history'][batch_type].append(batch_metrics)
        
        # Log batch metrics
        log_message(
            f"Batch Metrics - Type: {batch_type}, ID: {batch_id}, "
            f"Records: {records_count}, Time: {processing_time_ms:.2f}ms, "
            f"Rate: {batch_metrics['processing_rate']:.2f} rec/sec, "
            f"Batch CPU: {batch_cpu_percent:.1f}%, "
            f"CPU Diff: {cpu_percent_diff:.1f}%, "
            f"Memory Delta: {memory_diff_mb:.2f}MB"
        )
        
        # Write batch metrics to separate file for historical analysis
        try:
            batch_dir = f"{log_dir}/batches"
            os.makedirs(batch_dir, exist_ok=True)
            
            batch_file = f"{batch_dir}/{batch_type}_batch_{batch_id}_{int(time.time())}.json"
            with open(batch_file, "w") as f:
                json.dump(batch_metrics, f, indent=2)
        except Exception as e:
            log_message(f"Error writing batch metrics: {e}", "ERROR")
            stats['errors']['batch_metrics_write'] += 1
            
    except Exception as e:
        log_message(f"Error finishing batch metrics: {e}", "ERROR")
        stats['errors']['batch_metrics_finish'] += 1
        
        # Fall back to standard metrics collection
        record_batch_metrics(batch_type, batch_id, records_count, processing_time_ms, executor_metrics)

def record_batch_metrics(batch_type, batch_id, records_count, processing_time_ms, executor_metrics=None):
    """Record metrics for a specific batch (fallback method)"""
    # Collect system metrics at this point
    system_metrics = collect_system_metrics()
    
    # Create batch metrics record
    timestamp = datetime.datetime.now().isoformat()
    batch_metrics = {
        'batch_id': batch_id,
        'timestamp': timestamp,
        'records_count': records_count,
        'processing_time_ms': processing_time_ms,
        'processing_rate': (records_count / (processing_time_ms / 1000)) if processing_time_ms > 0 else 0,
        'system_metrics': system_metrics
    }
    
    # Add executor metrics if available
    if executor_metrics:
        batch_metrics['executor_metrics'] = executor_metrics
    
    # Add to batch history
    stats['batch_history'][batch_type].append(batch_metrics)
    
    # Log batch metrics
    log_message(
        f"Batch Metrics - Type: {batch_type}, ID: {batch_id}, "
        f"Records: {records_count}, Time: {processing_time_ms:.2f}ms, "
        f"Rate: {batch_metrics['processing_rate']:.2f} rec/sec, "
        f"System CPU: {system_metrics.get('cpu_percent', 'N/A')}%, "
        f"System Mem: {system_metrics.get('memory_percent', 'N/A')}%"
    )
    
    # Write batch metrics to separate file for historical analysis
    try:
        batch_dir = f"{log_dir}/batches"
        os.makedirs(batch_dir, exist_ok=True)
        
        batch_file = f"{batch_dir}/{batch_type}_batch_{batch_id}_{int(time.time())}.json"
        with open(batch_file, "w") as f:
            json.dump(batch_metrics, f, indent=2)
    except Exception as e:
        log_message(f"Error writing batch metrics: {e}", "ERROR")
        stats['errors']['batch_metrics_write'] += 1

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
            
            # Collect latest system metrics
            system_metrics = collect_system_metrics()
            
            # Get Spark UI metrics if available
            spark_metrics_display = ""
            spark_metrics = stats.get('spark_metrics', {})
            spark_ui_url = stats.get('spark_ui_url', 'N/A')
            
            if spark_metrics:
                active_jobs = spark_metrics.get('active_jobs', 0)
                executor_count = spark_metrics.get('executor_count', 0)
                memory_used_mb = spark_metrics.get('memory_used', 0) / (1024 * 1024)
                memory_total_mb = spark_metrics.get('memory_total', 0) / (1024 * 1024)
                memory_percent = (memory_used_mb / memory_total_mb * 100) if memory_total_mb > 0 else 0
                
                spark_metrics_display = f"""
            --- SPARK METRICS ---
            Spark UI: {spark_ui_url}
            Active Jobs: {active_jobs}
            Completed Jobs: {spark_metrics.get('completed_jobs', 0)}
            Failed Jobs: {spark_metrics.get('failed_jobs', 0)}
            Executors: {executor_count}
            Memory Usage: {memory_used_mb:.2f}MB / {memory_total_mb:.2f}MB ({memory_percent:.1f}%)
            """
                
                # Add streaming metrics if available
                if 'streaming_batches_total' in spark_metrics:
                    spark_metrics_display += f"""
            Streaming Batches: {spark_metrics.get('streaming_batches_total', 0)}
            Avg Processing Time: {spark_metrics.get('streaming_processing_time_avg', 0):.2f}ms
            """
            
            # Recent batch metrics info
            recent_batch_info = ""
            if stats['batch_history']['household'] or stats['batch_history']['ward']:
                recent_batch_info = "\n            --- RECENT BATCHES ---"
                
                # Show most recent household batch if available
                if stats['batch_history']['household']:
                    latest_household = stats['batch_history']['household'][-1]
                    batch_cpu = latest_household.get('batch_resources', {}).get('cpu_percent', 'N/A')
                    batch_cpu_diff = latest_household.get('batch_resources', {}).get('cpu_percent_diff', 'N/A')
                    batch_memory = latest_household.get('batch_resources', {}).get('memory_diff_mb', 'N/A')
                    
                    recent_batch_info += f"""
            Household (ID: {latest_household['batch_id']}): {latest_household['records_count']} records in {latest_household['processing_time_ms']:.2f}ms ({latest_household['processing_rate']:.2f} rec/sec)
            CPU: {batch_cpu if batch_cpu == 'N/A' else f"{batch_cpu:.1f}%"}, CPU Diff: {batch_cpu_diff if batch_cpu_diff == 'N/A' else f"{batch_cpu_diff:.1f}%"}, Memory Delta: {batch_memory if batch_memory == 'N/A' else f"{batch_memory:.2f}MB"}"""
                
                # Show most recent ward batch if available
                if stats['batch_history']['ward']:
                    latest_ward = stats['batch_history']['ward'][-1]
                    batch_cpu = latest_ward.get('batch_resources', {}).get('cpu_percent', 'N/A')
                    batch_cpu_diff = latest_ward.get('batch_resources', {}).get('cpu_percent_diff', 'N/A')
                    batch_memory = latest_ward.get('batch_resources', {}).get('memory_diff_mb', 'N/A')
                    
                    recent_batch_info += f"""
            Ward (ID: {latest_ward['batch_id']}): {latest_ward['records_count']} records in {latest_ward['processing_time_ms']:.2f}ms ({latest_ward['processing_rate']:.2f} rec/sec)
            CPU: {batch_cpu if batch_cpu == 'N/A' else f"{batch_cpu:.1f}%"}, CPU Diff: {batch_cpu_diff if batch_cpu_diff == 'N/A' else f"{batch_cpu_diff:.1f}%"}, Memory Delta: {batch_memory if batch_memory == 'N/A' else f"{batch_memory:.2f}MB"}"""
            
            # System resources info
            system_info = f"""
            --- SYSTEM RESOURCES ---
            CPU Usage: {system_metrics.get('cpu_percent', 'N/A')}%
            Memory Usage: {system_metrics.get('memory_percent', 'N/A')}% ({system_metrics.get('memory_used_mb', 0):.1f}MB / {system_metrics.get('memory_available_mb', 0) + system_metrics.get('memory_used_mb', 0):.1f}MB)
            Process ID: {CURRENT_PID}
            """
            
            # Construct message with only simple variables
            status = f"""
            ============= STREAMING JOB STATUS =============
            Runtime: {int(hours)}h {int(minutes)}m {int(seconds)}s
            Total Records: {total_records}
            Household Records: {household_records}
            Ward Records: {ward_records}
            Throughput: {records_per_second:.2f} records/second
            Error Count: {error_count}{spark_metrics_display}{recent_batch_info}{system_info}
            =================================================
            """
            # Use direct printing
            print(status)
            
            # Write complete stats to JSON file
            stats_copy = {
                'timestamp': datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                'runtime': int(runtime),
                'total_records': total_records,
                'household_records': household_records,
                'ward_records': ward_records,
                'error_count': error_count,
                'throughput': round(records_per_second, 2),
                'spark_metrics': spark_metrics,
                'system_metrics': system_metrics,
                # Include summary of recent batches
                'recent_batches': {
                    'household': list(stats['batch_history']['household'])[-5:] if stats['batch_history']['household'] else [],
                    'ward': list(stats['batch_history']['ward'])[-5:] if stats['batch_history']['ward'] else []
                }
            }
            
            with open(f"{log_dir}/stats_{int(time.time())}.json", "w") as f:
                json.dump(stats_copy, f, indent=2)
            
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
                        'durationMs': progress.get('triggerExecution', {}).get('durationMs', 0),
                        # Add batch ID if available
                        'batchId': progress.get('batchId', 'N/A'),
                    }
            
            # Check ward query status
            if ward_query and ward_query.isActive:
                progress = ward_query.lastProgress
                if progress:
                    query_statuses['ward'] = {
                        'inputRows': progress.get('numInputRows', 0),
                        'inputRowsPerSecond': progress.get('inputRowsPerSecond', 0),
                        'processedRowsPerSecond': progress.get('processedRowsPerSecond', 0),
                        'durationMs': progress.get('triggerExecution', {}).get('durationMs', 0),
                        # Add batch ID if available
                        'batchId': progress.get('batchId', 'N/A'),
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