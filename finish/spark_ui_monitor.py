# spark_ui_monitor.py
import threading
import time
import requests
import json
import os
from monitoring import log_message, stats

# Spark UI Configuration
SPARK_UI_HOST = os.environ.get("SPARK_UI_HOST", "localhost")
SPARK_UI_PORT = os.environ.get("SPARK_UI_PORT", "4044")
SPARK_UI_URL = f"http://{SPARK_UI_HOST}:{SPARK_UI_PORT}"

def check_spark_ui_availability():
    """Check if Spark UI is available"""
    try:
        response = requests.get(f"{SPARK_UI_URL}/api/v1/applications", timeout=5)
        return response.status_code == 200
    except Exception:
        return False

def get_active_jobs():
    """Get information about active Spark jobs"""
    try:
        response = requests.get(f"{SPARK_UI_URL}/api/v1/applications", timeout=5)
        if response.status_code == 200:
            apps = response.json()
            if apps:
                app_id = apps[0]['id']
                jobs_response = requests.get(f"{SPARK_UI_URL}/api/v1/applications/{app_id}/jobs", timeout=5)
                if jobs_response.status_code == 200:
                    return jobs_response.json()
        return []
    except Exception as e:
        log_message(f"Error fetching Spark jobs: {e}", "ERROR")
        stats['errors']['spark_ui_fetch'] += 1
        return []

def get_executor_metrics():
    """Get metrics about Spark executors"""
    try:
        response = requests.get(f"{SPARK_UI_URL}/api/v1/applications", timeout=5)
        if response.status_code == 200:
            apps = response.json()
            if apps:
                app_id = apps[0]['id']
                executors_response = requests.get(f"{SPARK_UI_URL}/api/v1/applications/{app_id}/executors", timeout=5)
                if executors_response.status_code == 200:
                    return executors_response.json()
        return []
    except Exception as e:
        log_message(f"Error fetching Spark executors: {e}", "ERROR")
        stats['errors']['spark_ui_fetch'] += 1
        return []

def get_streaming_statistics():
    """Get statistics about streaming queries"""
    try:
        response = requests.get(f"{SPARK_UI_URL}/api/v1/applications", timeout=5)
        if response.status_code == 200:
            apps = response.json()
            if apps:
                app_id = apps[0]['id']
                streaming_response = requests.get(f"{SPARK_UI_URL}/api/v1/applications/{app_id}/streaming/statistics", timeout=5)
                if streaming_response.status_code == 200:
                    return streaming_response.json()
        return {}
    except Exception as e:
        log_message(f"Error fetching streaming statistics: {e}", "ERROR")
        stats['errors']['spark_ui_fetch'] += 1
        return {}

def summarize_spark_metrics(jobs, executors, streaming_stats):
    """Create a summary of important Spark metrics"""
    summary = {
        "timestamp": time.time(),
        "active_jobs": len([j for j in jobs if j.get('status') == 'RUNNING']),
        "completed_jobs": len([j for j in jobs if j.get('status') == 'SUCCEEDED']),
        "failed_jobs": len([j for j in jobs if j.get('status') == 'FAILED']),
        "executor_count": len(executors),
        "total_cores": sum(e.get('totalCores', 0) for e in executors),
        "memory_used": sum(e.get('memoryUsed', 0) for e in executors),
        "memory_total": sum(e.get('maxMemory', 0) for e in executors),
    }
    
    # Add streaming metrics if available
    if streaming_stats and 'receivers' in streaming_stats:
        summary["streaming_receivers"] = len(streaming_stats.get('receivers', []))
        summary["streaming_batches_total"] = streaming_stats.get('totalCompletedBatches', 0)
        summary["streaming_processing_time_avg"] = streaming_stats.get('avgProcessingTime', 0)
        
    return summary

def monitor_spark_ui():
    """Thread to periodically monitor Spark UI metrics"""
    # Wait for Spark UI to become available
    ui_available = False
    retry_count = 0
    
    while not ui_available and retry_count < 10:
        ui_available = check_spark_ui_availability()
        if not ui_available:
            log_message(f"Waiting for Spark UI to become available (attempt {retry_count+1}/10)...", "INFO")
            retry_count += 1
            time.sleep(5)
    
    if not ui_available:
        log_message("Spark UI not available after 10 attempts, monitoring disabled", "WARNING")
        stats['errors']['spark_ui_unavailable'] += 1
        return
    
    log_message(f"Spark UI available at {SPARK_UI_URL}, starting monitoring")
    
    # Update global stats with Spark UI URL for reference
    stats['spark_ui_url'] = SPARK_UI_URL
    
    # Start monitoring loop
    while True:
        try:
            # Fetch metrics from Spark UI
            jobs = get_active_jobs()
            executors = get_executor_metrics()
            streaming_stats = get_streaming_statistics()
            
            # Create a summary of metrics
            metrics_summary = summarize_spark_metrics(jobs, executors, streaming_stats)
            
            # Add to stats
            stats['spark_metrics'] = metrics_summary
            
            # Log summary
            log_message(f"Spark Metrics: Active Jobs: {metrics_summary.get('active_jobs', 0)}, " 
                       f"Executors: {metrics_summary.get('executor_count', 0)}, "
                       f"Memory Used: {metrics_summary.get('memory_used', 0)/(1024*1024):.2f} MB / "
                       f"{metrics_summary.get('memory_total', 0)/(1024*1024):.2f} MB")
            
            # Sleep before next check
            time.sleep(30)
        except Exception as e:
            log_message(f"Error in Spark UI monitoring: {e}", "ERROR")
            stats['errors']['spark_ui_monitoring'] += 1
            time.sleep(10)

def start_spark_ui_monitoring():
    """Start the Spark UI monitoring thread"""
    spark_ui_thread = threading.Thread(target=monitor_spark_ui, daemon=True)
    spark_ui_thread.start()
    return log_message("Spark UI monitoring thread started")