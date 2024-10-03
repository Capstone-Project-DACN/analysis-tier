from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, min, max, avg, year, month
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType, TimestampType

# Create a Spark session
spark = SparkSession.builder \
    .appName("Kafka Example") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0") \
    .getOrCreate()

# Define Kafka source parameters
kafka_broker = "localhost:9092"
topic = "household_data"

# Define schema for the household data
household_schema = StructType([
    StructField("household_id", StringType()),
    StructField("timestamp", TimestampType()),
    StructField("electricity_usage_kwh", FloatType()),
    StructField("voltage", IntegerType()),
    StructField("current", FloatType()),
    StructField("location", StructType([
        StructField("house_number", StringType()),
        StructField("ward", StringType()),
        StructField("district", StringType())
    ])),
    StructField("price_per_kwh", IntegerType()),
    StructField("total_cost", IntegerType())
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

# Extract year and month from the timestamp
household_df = household_df.withColumn("year", year("timestamp")) \
                            .withColumn("month", month("timestamp"))

# Group by household_id, year, and month
result_df = household_df.groupBy("household_id", "year", "month") \
    .agg(
        min("electricity_usage_kwh").alias("min_electricity_usage"),
        max("electricity_usage_kwh").alias("max_electricity_usage"),
        avg("electricity_usage_kwh").alias("avg_electricity_usage"),
        min("voltage").alias("min_voltage"),
        max("voltage").alias("max_voltage"),
        avg("voltage").alias("avg_voltage")
    )

# Output to console (for debugging purposes)
query = result_df.writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

query.awaitTermination()
