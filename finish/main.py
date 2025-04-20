#main.py
import os
import time
import atexit
from pyspark.sql import SparkSession

# Import our modules
from schemas import household_schema, area_schema
from monitoring import log_message, stats, start_monitoring
from minio_handler import get_s3_client, initialize_buckets, write_data_to_minio, get_spark_minio_config
from kafka_handler import create_household_stream, create_ward_stream, start_streaming_queries
from spark_ui_monitor import start_spark_ui_monitoring
from spark_dashboard import start_dashboard

def main():
    # Get absolute path to the jars directory
    jars_dir = os.path.abspath("./jars")
    jars = ",".join([
        f"{jars_dir}/hadoop-aws-3.3.1.jar",
        f"{jars_dir}/aws-java-sdk-bundle-1.11.901.jar",
        f"{jars_dir}/wildfly-openssl-1.0.7.Final.jar"
    ])
    
    # Create a Spark session with optimized MinIO configuration
    log_message("Initializing Spark session...")
    spark = SparkSession.builder \
        .appName("Kafka to MinIO Electricity Data") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0") \
        .config("spark.sql.shuffle.partitions", "10") \
        .config("spark.default.parallelism", "10") \
        .config("spark.sql.adaptive.enabled", "true")
    
    # Add MinIO configurations
    for key, value in get_spark_minio_config(jars).items():
        spark = spark.config(key, value)
    
    # Create the SparkSession
    spark = spark.getOrCreate()
    
    # Set error-only logging for Spark system logs
    spark.sparkContext.setLogLevel("ERROR")
    
    # Initialize the buckets in MinIO
    log_message("Initializing MinIO buckets...")
    s3_client = get_s3_client()
    buckets = {
        'household': 'household',
        'ward': 'ward'
    }
    initialize_buckets(s3_client, buckets.values())
    
    # Create DataFrames from Kafka streams
    log_message("Creating Kafka streams...")
    household_df = create_household_stream(spark, household_schema)
    ward_df = create_ward_stream(spark, area_schema)
    
    # Start the streams
    log_message("Starting streaming queries...")
    household_query, ward_query = start_streaming_queries(
        household_df, 
        ward_df, 
        write_data_to_minio, 
        spark, 
        buckets
    )
    
    # Start monitoring threads
    log_message("Starting monitoring threads...")
    start_monitoring(
        household_query=household_query,
        ward_query=ward_query,
        s3_client=s3_client,
        buckets=buckets.values()
    )
    
    # Start Spark UI monitoring
    log_message("Starting Spark UI monitoring...")
    start_spark_ui_monitoring()
    
    # Start dashboard
    log_message("Starting dashboard web interface...")
    start_dashboard()
    
    # Register cleanup handler
    def cleanup():
        """Cleanup function to be called on exit"""
        log_message("Shutting down streaming application...")
        
        # Stop all queries
        if ward_query.isActive:
            try:
                ward_query.stop()
                log_message("Stopped ward query")
            except:
                pass
        
        if household_query.isActive:
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
    Household Records: {stats['household_records_processed']}
    Ward Records: {stats['ward_records_processed']}
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
        cleanup()
    except Exception as e:
        log_message(f"Error during query execution: {e}", "ERROR")
        stats['errors']['query_execution'] += 1
        cleanup()

if __name__ == "__main__":
    main()