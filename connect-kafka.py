from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, min, max, avg, year, month,sum
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType, TimestampType
import time
import os

# Create a Spark session + MinIO configuration
spark = SparkSession.builder \
    .appName("Kafka Example") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "myminioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "myminioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.jars", jars) \
    .getOrCreate()

# Define Kafka source parameters
kafka_broker = "localhost:9092"
topic = "household_data"

# Get absolute path to the jars directory
jars_dir = os.path.abspath("./jars")
jars = ",".join([
    f"{jars_dir}/hadoop-aws-3.3.1.jar",
    f"{jars_dir}/aws-java-sdk-bundle-1.11.901.jar",
    f"{jars_dir}/wildfly-openssl-1.0.7.Final.jar"
])

# Define schema for the household data
household_schema = StructType([
    StructField("household_id", StringType(), nullable=False),
    StructField("timestamp", TimestampType(), nullable=False),
    StructField("electricity_usage_kwh", FloatType(), nullable=False),
    StructField("voltage", IntegerType(), nullable=False),
    StructField("current", FloatType(), nullable=False),
    StructField("location", StructType([
        StructField("house_number", StringType(), nullable=False),
        StructField("ward", StringType(), nullable=False),
        StructField("district", StringType(), nullable=False)
    ]), nullable=False),
    StructField("price_per_kwh", IntegerType(), nullable=False),
    StructField("total_cost", IntegerType(), nullable=False)
])



# Read data from Kafka
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_broker) \
    .option("subscribe", topic) \
    .option("startingOffsets", "earliest") \
    .load()

# Convert the Kafka value column (binary) to a string and parse the JSON
household_df = kafka_df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), household_schema).alias("data")) \
    .select("data.*")
    
household_df.printSchema()

# Extract year and month from the timestamp
household_df = household_df.withColumn("year", year("timestamp")) \
                            .withColumn("month", month("timestamp"))
household_df = household_df.withColumn("electricity_usage_kwh", col("electricity_usage_kwh").cast(FloatType()))

# Group by household_id, year, and month
house_data = household_df.groupBy("household_id", "year", "month") \
    .agg(
        min("electricity_usage_kwh").alias("min_electricity_usage"),
        max("electricity_usage_kwh").alias("max_electricity_usage"),
        avg("electricity_usage_kwh").alias("avg_electricity_usage"),
        min("voltage").alias("min_voltage"),
        max("voltage").alias("max_voltage"),
        avg("voltage").alias("avg_voltage")
    )
    
district_data = household_df.groupBy("location.district", "year", "month") \
    .agg(
        sum("electricity_usage_kwh").alias("total_electricity_usage")
    )



# Output to console (for debugging purposes)
house_data_query = house_data.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", "false") \
    .start()

# Output district-level data to console
district_data_query = district_data.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", "false") \
    .start()

### create the bucket name with format: district-ward-house_number (if not exist) then insert the data
### household_df electricity_usage_kwh + timestamp in yyyy-mm-dd hour-minute format 

### create the bucket name with format: district-ward-house_number_year_month_average (if not exist) then insert the data
### household_df min_electricity_usage + max_electricity_usage + avg_electricity_usage +  timestamp in yyyy-mm  format 




# Write the DataFrame to MinIO
try:
    print(f"Writing data to {file_path}")
    test_df.write.parquet(file_path)
    print("Successfully wrote data to MinIO!")

    # Verify by reading it back
    print("Reading data back from MinIO...")
    read_df = spark.read.parquet(file_path)
    print("Data read from MinIO:")
    read_df.show()
    
except Exception as e:
    print(f"Error connecting to MinIO: {e}")
    
# Wait for both queries to finish
house_data_query.awaitTermination()
district_data_query.awaitTermination()         