# spark_dashboard.py
import os
import json
import time
import threading
import socket
from http.server import HTTPServer, SimpleHTTPRequestHandler
from datetime import datetime
import glob
from monitoring import stats, log_message

# Dashboard configuration
DASHBOARD_HOST = os.environ.get("DASHBOARD_HOST", "0.0.0.0")  # Bind to all interfaces
DASHBOARD_PORT = int(os.environ.get("DASHBOARD_PORT", "4041"))
DASHBOARD_UPDATE_INTERVAL = int(os.environ.get("DASHBOARD_UPDATE_INTERVAL", "5"))

# Create dashboard directory
dashboard_dir = "./dashboard"
os.makedirs(dashboard_dir, exist_ok=True)

def generate_dashboard_html():
    """Generate HTML dashboard from current stats"""
    # Create timestamp for the dashboard
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    # Calculate runtime
    runtime = time.time() - stats['start_time']
    hours, remainder = divmod(runtime, 3600)
    minutes, seconds = divmod(remainder, 60)
    runtime_str = f"{int(hours)}h {int(minutes)}m {int(seconds)}s"
    
    # Calculate average throughput
    total_records = stats['total_records_processed']
    throughput = total_records / (runtime if runtime > 0 else 1)
    
    # Get error count
    error_count = sum(stats['errors'].values())
    
    # Get Spark UI info
    spark_ui_url = stats.get('spark_ui_url', '#')
    spark_metrics = stats.get('spark_metrics', {})
    
    # Get system metrics
    system_metrics = stats.get('system_metrics', {})
    
    # Get batch history
    batch_history = stats.get('batch_history', {'household': [], 'ward': []})
    
    # Create HTML content
    html = f"""<!DOCTYPE html>
<html>
<head>
    <title>Spark Streaming Dashboard</title>
    <meta http-equiv="refresh" content="{DASHBOARD_UPDATE_INTERVAL}">
    <style>
        body {{ font-family: Arial, sans-serif; margin: 0; padding: 20px; background-color: #f5f5f5; }}
        .dashboard {{ max-width: 1200px; margin: 0 auto; background-color: white; border-radius: 5px; box-shadow: 0 2px 5px rgba(0,0,0,0.1); padding: 20px; }}
        .header {{ display: flex; justify-content: space-between; align-items: center; margin-bottom: 20px; border-bottom: 1px solid #eee; padding-bottom: 10px; }}
        .card {{ background-color: white; border-radius: 5px; box-shadow: 0 2px 5px rgba(0,0,0,0.1); padding: 15px; margin-bottom: 20px; }}
        .card-row {{ display: flex; flex-wrap: wrap; gap: 20px; margin-bottom: 20px; }}
        .card-col {{ flex: 1; min-width: 200px; }}
        .metric {{ padding: 15px; border-radius: 5px; margin-bottom: 10px; }}
        .metric-primary {{ background-color: #e3f2fd; }}
        .metric-secondary {{ background-color: #f5f5f5; }}
        .metric-success {{ background-color: #e8f5e9; }}
        .metric-warning {{ background-color: #fff8e1; }}
        .metric-danger {{ background-color: #ffebee; }}
        .metric h3 {{ margin-top: 0; color: #555; font-size: 14px; }}
        .metric p {{ margin-bottom: 0; font-size: 24px; font-weight: bold; color: #333; }}
        .spark-ui-link {{ display: inline-block; margin-top: 10px; background-color: #2196F3; color: white; padding: 5px 10px; text-decoration: none; border-radius: 3px; }}
        .spark-ui-link:hover {{ background-color: #1976D2; }}
        table {{ width: 100%; border-collapse: collapse; }}
        th, td {{ text-align: left; padding: 8px; border-bottom: 1px solid #ddd; }}
        th {{ background-color: #f5f5f5; }}
        .table-container {{ max-height: 300px; overflow-y: auto; margin-top: 15px; }}
        .error-table {{ max-height: 200px; overflow-y: auto; }}
        .progress-bar {{ 
            height: 20px; 
            background-color: #e0e0e0; 
            border-radius: 10px; 
            overflow: hidden; 
            margin-top: 5px; 
        }}
        .progress-bar-fill {{ 
            height: 100%; 
            background-color: #4caf50; 
            border-radius: 10px; 
        }}
        .progress-bar-fill.warning {{ background-color: #ff9800; }}
        .progress-bar-fill.danger {{ background-color: #f44336; }}
    </style>
</head>
<body>
    <div class="dashboard">
        <div class="header">
            <h1>Spark Streaming Dashboard</h1>
            <p>Last updated: {timestamp}</p>
        </div>
        
        <div class="card">
            <h2>Application Overview</h2>
            <div class="card-row">
                <div class="card-col">
                    <div class="metric metric-primary">
                        <h3>Runtime</h3>
                        <p>{runtime_str}</p>
                    </div>
                    <div class="metric metric-success">
                        <h3>Total Records</h3>
                        <p>{total_records:,}</p>
                    </div>
                </div>
                <div class="card-col">
                    <div class="metric metric-primary">
                        <h3>Throughput</h3>
                        <p>{throughput:.2f} records/sec</p>
                    </div>
                    <div class="metric {'metric-danger' if error_count > 0 else 'metric-success'}">
                        <h3>Errors</h3>
                        <p>{error_count}</p>
                    </div>
                </div>
                <div class="card-col">
                    <div class="metric metric-secondary">
                        <h3>Household Records</h3>
                        <p>{stats['household_records_processed']:,}</p>
                    </div>
                    <div class="metric metric-secondary">
                        <h3>Ward Records</h3>
                        <p>{stats['ward_records_processed']:,}</p>
                    </div>
                </div>
            </div>
        </div>
        
        <div class="card">
            <h2>System Resources</h2>
            <div class="card-row">
                <div class="card-col">
                    <div class="metric metric-primary">
                        <h3>CPU Usage</h3>
                        <p>{system_metrics.get('cpu_percent', 0)}%</p>
                        <div class="progress-bar">
                            <div class="progress-bar-fill {'danger' if system_metrics.get('cpu_percent', 0) > 80 else 'warning' if system_metrics.get('cpu_percent', 0) > 60 else ''}" style="width: {min(system_metrics.get('cpu_percent', 0), 100)}%;"></div>
                        </div>
                    </div>
                </div>
                <div class="card-col">
                    <div class="metric metric-primary">
                        <h3>Memory Usage</h3>
                        <p>{system_metrics.get('memory_usage_percent', 0)}%</p>
                        <div class="progress-bar">
                            <div class="progress-bar-fill {'danger' if system_metrics.get('memory_usage_percent', 0) > 80 else 'warning' if system_metrics.get('memory_usage_percent', 0) > 60 else ''}" style="width: {min(system_metrics.get('memory_usage_percent', 0), 100)}%;"></div>
                        </div>
                    </div>
                </div>
                <div class="card-col">
                    <div class="metric metric-secondary">
                        <h3>Memory Consumption</h3>
                        <p>{system_metrics.get('memory_usage_mb', 0):.1f}MB / {system_metrics.get('memory_available_mb', 0) + system_metrics.get('memory_usage_mb', 0):.1f}MB</p>
                    </div>
                </div>
            </div>
        </div>
        
        <div class="card">
            <h2>Spark Metrics</h2>"""
    
    if spark_metrics:
        # Calculate memory metrics
        memory_used_mb = spark_metrics.get('memory_used', 0) / (1024 * 1024)
        memory_total_mb = spark_metrics.get('memory_total', 0) / (1024 * 1024)
        memory_percent = (memory_used_mb / memory_total_mb * 100) if memory_total_mb > 0 else 0
        
        html += f"""
            <div class="card-row">
                <div class="card-col">
                    <div class="metric metric-primary">
                        <h3>Active Jobs</h3>
                        <p>{spark_metrics.get('active_jobs', 0)}</p>
                    </div>
                    <div class="metric metric-success">
                        <h3>Completed Jobs</h3>
                        <p>{spark_metrics.get('completed_jobs', 0)}</p>
                    </div>
                </div>
                <div class="card-col">
                    <div class="metric metric-primary">
                        <h3>Executors</h3>
                        <p>{spark_metrics.get('executor_count', 0)}</p>
                    </div>
                    <div class="metric {'metric-warning' if memory_percent > 75 else 'metric-success'}">
                        <h3>Spark Memory Usage</h3>
                        <p>{memory_used_mb:.1f}MB / {memory_total_mb:.1f}MB ({memory_percent:.1f}%)</p>
                        <div class="progress-bar">
                            <div class="progress-bar-fill {'danger' if memory_percent > 80 else 'warning' if memory_percent > 60 else ''}" style="width: {min(memory_percent, 100)}%;"></div>
                        </div>
                    </div>
                </div>
            </div>
            <a href="{spark_ui_url}" target="_blank" class="spark-ui-link">Open Spark UI</a>"""
    else:
        html += """
            <p>No Spark metrics available yet.</p>"""
    
    html += """
        </div>
        
        <div class="card">
            <h2>Batch Processing Metrics</h2>
            <div class="card-row">
                <div class="card-col">
                    <h3>Household Batches</h3>"""
    
    # Household batch history
    if batch_history['household']:
        html += """
                    <div class="table-container">
                        <table>
                            <thead>
                                <tr>
                                    <th>Batch ID</th>
                                    <th>Records</th>
                                    <th>Time (ms)</th>
                                    <th>Rate (rec/sec)</th>
                                    <th>CPU Usage</th>
                                    <th>Memory Usage</th>
                                </tr>
                            </thead>
                            <tbody>"""
        
        # Sort batches by timestamp, newest first
        sorted_batches = sorted(
            list(batch_history['household']), 
            key=lambda x: x.get('timestamp', ''), 
            reverse=True
        )
        
        for batch in sorted_batches[:10]:  # Show last 10 batches
            # Get batch-specific resource metrics
            batch_resources = batch.get('batch_resources', {})
            cpu_diff = batch_resources.get('cpu_percent_diff', 'N/A')
            if cpu_diff != 'N/A':
                cpu_diff = f"{cpu_diff:.1f}%"
                
            memory_diff = batch_resources.get('memory_diff_mb', 'N/A')
            if memory_diff != 'N/A':
                memory_diff = f"{memory_diff:.2f}MB"
            
            html += f"""
                                <tr>
                                    <td>{batch.get('batch_id', 'N/A')}</td>
                                    <td>{batch.get('records_count', 0):,}</td>
                                    <td>{batch.get('processing_time_ms', 0):.1f}</td>
                                    <td>{batch.get('processing_rate', 0):.1f}</td>
                                    <td>{cpu_diff}</td>
                                    <td>{memory_diff}</td>
                                </tr>"""
        
        html += """
                            </tbody>
                        </table>
                    </div>"""
    else:
        html += """
                    <p>No household batch history available yet.</p>"""
    
    html += """
                </div>
                <div class="card-col">
                    <h3>Ward Batches</h3>"""
    
    # Ward batch history
    if batch_history['ward']:
        html += """
                    <div class="table-container">
                        <table>
                            <thead>
                                <tr>
                                    <th>Batch ID</th>
                                    <th>Records</th>
                                    <th>Time (ms)</th>
                                    <th>Rate (rec/sec)</th>
                                    <th>CPU Usage</th>
                                    <th>Memory Usage</th>
                                </tr>
                            </thead>
                            <tbody>"""
        
        # Sort batches by timestamp, newest first
        sorted_batches = sorted(
            list(batch_history['ward']), 
            key=lambda x: x.get('timestamp', ''), 
            reverse=True
        )
        
        for batch in sorted_batches[:10]:  # Show last 10 batches
            # Get batch-specific resource metrics
            batch_resources = batch.get('batch_resources', {})
            cpu_diff = batch_resources.get('cpu_percent_diff', 'N/A')
            if cpu_diff != 'N/A':
                cpu_diff = f"{cpu_diff:.1f}%"
                
            memory_diff = batch_resources.get('memory_diff_mb', 'N/A')
            if memory_diff != 'N/A':
                memory_diff = f"{memory_diff:.2f}MB"
            
            html += f"""
                                <tr>
                                    <td>{batch.get('batch_id', 'N/A')}</td>
                                    <td>{batch.get('records_count', 0):,}</td>
                                    <td>{batch.get('processing_time_ms', 0):.1f}</td>
                                    <td>{batch.get('processing_rate', 0):.1f}</td>
                                    <td>{cpu_diff}</td>
                                    <td>{memory_diff}</td>
                                </tr>"""
        
        html += """
                            </tbody>
                        </table>
                    </div>"""
    else:
        html += """
                    <p>No ward batch history available yet.</p>"""
    
    html += """
                </div>
            </div>
        </div>
        
        <div class="card">
            <h2>Streaming Query Status</h2>
            <div class="card-row">
                <div class="card-col">
                    <h3>Household Query</h3>
                    <div class="metric metric-secondary">
                        <h3>Records Processed</h3>
                        <p>"""
    
    html += f"{stats['household_records_processed']:,}</p>"
    html += """
                    </div>
                    <div class="metric metric-secondary">
                        <h3>Batches</h3>
                        <p>"""
    
    html += f"{stats['household_batches_processed']:,}</p>"
    html += """
                    </div>
                </div>
                <div class="card-col">
                    <h3>Ward Query</h3>
                    <div class="metric metric-secondary">
                        <h3>Records Processed</h3>
                        <p>"""
    
    html += f"{stats['ward_records_processed']:,}</p>"
    html += """
                    </div>
                    <div class="metric metric-secondary">
                        <h3>Batches</h3>
                        <p>"""
    
    html += f"{stats['ward_batches_processed']:,}</p>"
    html += """
                    </div>
                </div>
            </div>
        </div>
        
        <div class="card">
            <h2>Error Log</h2>"""
    
    if error_count > 0:
        html += """
            <div class="error-table">
                <table>
                    <thead>
                        <tr>
                            <th>Error Type</th>
                            <th>Count</th>
                        </tr>
                    </thead>
                    <tbody>"""
        
        for error_type, count in stats['errors'].items():
            html += f"""
                        <tr>
                            <td>{error_type}</td>
                            <td>{count}</td>
                        </tr>"""
        
        html += """
                    </tbody>
                </table>
            </div>"""
    else:
        html += """
            <p>No errors reported.</p>"""
    
    html += """
        </div>
    </div>
</body>
</html>"""
    
    return html

def write_dashboard_file():
    """Write dashboard HTML to file"""
    html_content = generate_dashboard_html()
    with open(f"{dashboard_dir}/index.html", "w") as f:
        f.write(html_content)
    return True

class DashboardHandler(SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=dashboard_dir, **kwargs)
    
    def log_message(self, format, *args):
        # Redirect server logs to our logging system
        log_message(f"Dashboard HTTP: {format % args}", "DEBUG")

def serve_dashboard():
    """Serve dashboard on HTTP server with port auto-selection"""
    global DASHBOARD_PORT
    max_attempts = 10
    
    for attempt in range(max_attempts):
        try:
            server_address = (DASHBOARD_HOST, DASHBOARD_PORT)
            httpd = HTTPServer(server_address, DashboardHandler)
            # Add socket reuse to avoid "Address already in use" errors
            httpd.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            log_message(f"Starting dashboard HTTP server on http://{DASHBOARD_HOST}:{DASHBOARD_PORT}")
            httpd.serve_forever()
        except OSError as e:
            if e.errno == 48 or "Address already in use" in str(e):  # Address already in use
                log_message(f"Port {DASHBOARD_PORT} is in use, trying {DASHBOARD_PORT + 1}")
                DASHBOARD_PORT += 1
                if attempt == max_attempts - 1:
                    log_message(f"Failed to start dashboard after {max_attempts} attempts", "ERROR")
                    return
            else:
                log_message(f"Error starting dashboard server: {e}", "ERROR")
                return

def update_dashboard():
    """Thread to periodically update dashboard HTML"""
    while True:
        try:
            write_dashboard_file()
            time.sleep(DASHBOARD_UPDATE_INTERVAL)
        except Exception as e:
            log_message(f"Error updating dashboard: {e}", "ERROR")
            time.sleep(10)

def start_dashboard():
    """Start dashboard threads"""
    # Create initial dashboard file
    write_dashboard_file()
    
    # Start update thread
    update_thread = threading.Thread(target=update_dashboard, daemon=True)
    update_thread.start()
    
    # Start HTTP server thread
    server_thread = threading.Thread(target=serve_dashboard, daemon=True)
    server_thread.start()
    
    log_message(f"Dashboard started at http://{DASHBOARD_HOST}:{DASHBOARD_PORT}")
    
    return True

if __name__ == "__main__":
    # This allows running the dashboard standalone for testing
    start_dashboard()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("Dashboard stopped")