from pyspark.sql import SparkSession

# Create a Spark session
spark = SparkSession.builder \
    .appName("KafkaSparkConsumer") \
    .getOrCreate()

# Define Kafka source parameters
kafka_broker = "localhost:9092"
topic = "your_topic_name"  # Replace with your actual topic

# Read data from Kafka
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_broker) \
    .option("subscribe", topic) \
    .option("startingOffsets", "earliest") \
    .load()

# Convert the value column from binary to string
messages_df = kafka_df.selectExpr("CAST(value AS STRING)")

# Write the stream to console (or any sink)
query = messages_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

# Await termination
query.awaitTermination()