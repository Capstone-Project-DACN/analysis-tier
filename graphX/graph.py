from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from graphframes import GraphFrame

spark = SparkSession.builder \
    .appName("Electricity-GraphX") \
    .config("spark.jars.packages", "graphframes:graphframes:0.8.2-spark3.0-s_2.12") \
    .config("spark.executor.memory", "4g") \
    .config("spark.driver.memory", "4g") \
    .config("spark.sql.shuffle.partitions", "50") \
    .getOrCreate()

sc = spark.sparkContext
sc.setCheckpointDir("/tmp/spark_checkpoint")

household_data = [
    ("A1B2C3", "2024-09-29T10:00:00Z", 4.75, 230, 10.5, "268", "phuong_14", "quan_10", 1800, 8550),
    ("D4E5F6", "2024-09-29T11:00:00Z", 3.5, 230, 9.8, "123", "phuong_15", "quan_11", 1800, 6300),
    ("G7H8I9", "2024-09-29T12:00:00Z", 6.0, 230, 12.2, "567", "phuong_14", "quan_10", 1800, 10800),
]

columns = ["household_id", "timestamp", "electricity_usage_kwh", "voltage", "current", "house_number", "ward", "district", "price_per_kwh", "total_cost"]
household_df = spark.createDataFrame(household_data, columns)

# define transformer and connect to address
transformers = [
    ("T1", "quan_10", "phuong_14"),   
    ("T2", "quan_11", "phuong_15"),   
]

transformer_df = spark.createDataFrame(transformers, ["transformer_id", "district", "ward"])
# transformer_df.show()

edges = household_df.join(transformer_df, on=["district", "ward"]) \
    .select(
        F.col("household_id").alias("src"),
        F.col("transformer_id").alias("dst"),
        F.lit(1).alias("availability")  # Availability set to 1 initially
    )
# Show edges (connections)
# edges.show()

households_vertices = household_df.select(F.col("household_id").alias("id"))
transformer_vertices = transformer_df.select(F.col("transformer_id").alias("id"))

vertices = households_vertices.union(transformer_vertices)

graph = GraphFrame(vertices, edges)

# PageRank: Rank vertices based on their centrality in the graph
print("================================ Calculating Pagerank ================================")
pagerank_result = graph.pageRank(resetProbability=0.15, maxIter=10)
pagerank_result.vertices.select("id", "pagerank").orderBy(F.col("pagerank").desc()).show()
print("================================ Grouping Component ================================")
connected_components = graph.connectedComponents()
connected_components.select("id", "component").orderBy(F.col("component").asc()).show()




# graph.vertices.show()
# graph.edges.show()

# def simulate_transformer_failure(transformer_id):
#     failed_edges = edges.withColumn(
#         "availability",
#         F.when(F.col("dst") == transformer_id, 0).otherwise(F.col("availability"))
#     )
#     return failed_edges

# edges_after_failure = simulate_transformer_failure("T1")

# edges_after_failure.show()

# total_households = edges_after_failure.filter(F.col("src").isNotNull()).count()
# available_households = edges_after_failure.filter((F.col("src").isNotNull()) & (F.col("availability") == 1)).count()

# print(f"Total households: {total_households}")
# print(f"Households with electricity: {available_households}")

# households_vertices.show()
# transformer_vertices.show()
# vertices.show()