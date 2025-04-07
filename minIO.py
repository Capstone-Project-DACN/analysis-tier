from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import time
import os

# Get absolute path to the jars directory
jars_dir = os.path.abspath("./jars")
jars = ",".join([
    f"{jars_dir}/hadoop-aws-3.3.1.jar",
    f"{jars_dir}/aws-java-sdk-bundle-1.11.901.jar",
    f"{jars_dir}/wildfly-openssl-1.0.7.Final.jar"
])

# Create a Spark session with MinIO configurations
spark = SparkSession.builder \
    .appName("MinIO Connection Test") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "myminioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "myminioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.jars", jars) \
    .getOrCreate()
    
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

# Define the bucket and path for the test file
bucket_name = "abc"  # Make sure this bucket exists in MinIO
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

# Stop the Spark session
spark.stop()