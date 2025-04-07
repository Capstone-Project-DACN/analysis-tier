from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
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
bucket_name = "new-mexico-morissettehaven-49314-fabiola-hill"

# Function to check if bucket exists and create if it doesn't
def ensure_bucket_exists(bucket_name):
    try:
        # Create a connection to MinIO
        s3_client = boto3.client(
            's3',
            endpoint_url=endpoint_url,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            config=Config(signature_version='s3v4'),
            region_name='us-east-1'  # This is not used by MinIO but required by boto3
        )
        
        # Check if bucket exists
        existing_buckets = s3_client.list_buckets()
        bucket_exists = any(bucket['Name'] == bucket_name for bucket in existing_buckets['Buckets'])
        
        if not bucket_exists:
            print(f"Bucket '{bucket_name}' does not exist. Creating it...")
            s3_client.create_bucket(Bucket=bucket_name)
            print(f"Bucket '{bucket_name}' created successfully.")
        else:
            print(f"Bucket '{bucket_name}' already exists.")
            
        return True
    except Exception as e:
        print(f"Error checking/creating bucket: {e}")
        return False

# Create a Spark session with MinIO configurations
spark = SparkSession.builder \
    .appName("MinIO Connection Test") \
    .config("spark.hadoop.fs.s3a.endpoint", endpoint_url) \
    .config("spark.hadoop.fs.s3a.access.key", access_key) \
    .config("spark.hadoop.fs.s3a.secret.key", secret_key) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.jars", jars) \
    .getOrCreate()

# Ensure bucket exists before proceeding
if ensure_bucket_exists(bucket_name):
    # Create a simple test DataFrame
    data = [("test1", 1), ("test2", 2), ("test3", 3)]
    schema = StructType([
        StructField("name", StringType(), False),
        StructField("value", IntegerType(), False)
    ])
    test_df = spark.createDataFrame(data, schema)

    # Show the DataFrame
    print("Test DataFrame:")
    test_df.show()

    # Define the path for the test file
    timestamp = int(time.time())
    file_path = f"s3a://{bucket_name}/test-data-{timestamp}.txt"

    # Write the DataFrame to MinIO as text
    try:
        print(f"Writing data to {file_path}")
        
        # For text files, we need to convert the DataFrame to a single string column
        # The easiest way is to convert the rows to strings
        from pyspark.sql.functions import concat_ws, col
        
        # Convert all columns to string and concatenate them with a delimiter
        text_df = test_df.select(concat_ws(',', *[col(c).cast("string") for c in test_df.columns]).alias("value"))
        
        # Save as text file
        text_df.write.text(file_path)
        print("Successfully wrote data to MinIO as text!")

        # Verify by reading it back
        print("Reading data back from MinIO...")
        read_df = spark.read.text(file_path)
        print("Data read from MinIO:")
        read_df.show(truncate=False)
        
    except Exception as e:
        print(f"Error connecting to MinIO: {e}")
else:
    print("Failed to ensure bucket exists. Aborting operations.")

# Stop the Spark session
spark.stop()