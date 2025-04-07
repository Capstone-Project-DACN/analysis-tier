from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, struct, lit
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType
from graphframes import GraphFrame

# Initialize Spark session
spark = SparkSession.builder \
    .appName("ElectricityAvailability") \
    .getOrCreate()

# Kafka configuration
kafka_brokers = "localhost:9092"
topic = "electricity_usage"

# Define the schema for the incoming data
schema = StructType([
    StructField("household_id", StringType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("electricity_usage_kwh", DoubleType(), True),
    StructField("voltage", IntegerType(), True),
    StructField("current", DoubleType(), True),
    StructField("location", StructType([
        StructField("house_number", StringType(), True),
        StructField("ward", StringType(), True),
        StructField("district", StringType(), True)
    ]), True),
    StructField("price_per_kwh", IntegerType(), True),
    StructField("total_cost", IntegerType(), True)
])

# Read from Kafka
kafka_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_brokers) \
    .option("subscribe", topic) \
    .load()

# Parse the Kafka value column, which contains the JSON
parsed_stream = kafka_stream.select(from_json(col("value").cast("string"), schema).alias("data"))

# Extract fields
household_data = parsed_stream.select(
    col("data.household_id"),
    col("data.timestamp"),
    col("data.electricity_usage_kwh"),
    col("data.voltage"),
    col("data.current"),
    col("data.location.house_number"),
    col("data.location.ward"),
    col("data.location.district"),
    col("data.price_per_kwh"),
    col("data.total_cost")
)

# Step 2: Verify if the household is out of electricity
def is_out_of_electricity(row):
    return 1 if row["voltage"] > 0 else 0  # If voltage is zero, it's out of electricity

# Add availability column
household_data = household_data.withColumn("availability", lit(1))  # Default availability

# Step 3: Define transformers and connect households
transformer_mapping = {
    ("quan_10", "phuong_14"): "Transformer_1",
    ("quan_5", "phuong_3"): "Transformer_2",
    # Add more mappings as needed
}

def assign_transformer(row):
    return transformer_mapping.get((row["district"], row["ward"]), "Unknown_Transformer")

# Add transformer column
household_data = household_data.withColumn("transformer", lit("Transformer_1"))  # Default transformer

# GraphX Part: Build graph from households and transformers
# Create vertices and edges
vertices = household_data.select("household_id", "house_number", "ward", "district", "availability")
transformer_vertices = spark.createDataFrame([
    ("Transformer_1", "quan_10", "phuong_14"),
    ("Transformer_2", "quan_5", "phuong_3"),
], ["transformer_id", "district", "ward"])

# Combine households and transformers into one vertex dataframe
combined_vertices = vertices.unionAll(transformer_vertices)

# Define edges to connect households to transformers
edges = spark.createDataFrame([
    ("household_id_1", "Transformer_1", "connected"),
    ("household_id_2", "Transformer_2", "connected"),
], ["src", "dst", "relationship"])

# Create the graph
graph = GraphFrame(combined_vertices, edges)

# Step 4: Show availability of each household
output = household_data.select("household_id", struct("house_number", "ward", "district").alias("address"), "availability")

# Step 5: Repeat the process for new messages
query = output.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query.awaitTermination()
