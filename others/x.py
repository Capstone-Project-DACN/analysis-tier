from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from graphframes import GraphFrame

# Initialize Spark session with increased memory settings
spark = SparkSession.builder \
    .appName("Electricity Usage Graph") \
    .config("spark.jars.packages", "graphframes:graphframes:0.8.2-spark3.1-s_2.12") \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.memory", "4g") \
    .getOrCreate()

# Set checkpoint directory (ensure it's valid)
spark.sparkContext.setCheckpointDir("/tmp/checkpoints")

# Example electricity usage data
household_data = [
    ("A1B2C3", "2024-09-29T10:00:00Z", 4.75, 230, 10.5, "268", "phuong_14", "quan_10", 1800, 8550),
    ("D4E5F6", "2024-09-29T10:10:00Z", 3.25, 235, 9.0, "269", "phuong_14", "quan_10", 1800, 5850),
    ("G7H8I9", "2024-09-29T10:20:00Z", 5.0, 220, 11.0, "270", "phuong_15", "quan_11", 1800, 9000)
]

# Create a DataFrame for vertices (households)
vertices_df = spark.createDataFrame(household_data, ["household_id", "timestamp", "electricity_usage_kwh", "voltage", "current", "house_number", "ward", "district", "price_per_kwh", "total_cost"])

# Add an 'id' column to be used as a unique identifier for each vertex
vertices_df = vertices_df.withColumn("id", col("household_id"))

# Create edges based on proximity (same ward means they're neighbors)
edges_data = [
    ("A1B2C3", "D4E5F6", 1.5),  # Example: Usage difference between two neighbors
    ("A1B2C3", "G7H8I9", 3.0),  # Not direct neighbors, but still have an edge
]

edges_df = spark.createDataFrame(edges_data, ["src", "dst", "usage_diff"])

# Create the graph
graph = GraphFrame(vertices_df, edges_df)

# Run connected components
connected_components = graph.connectedComponents()
connected_components.select("id", "component").show()

# Stop the Spark session
spark.stop()
