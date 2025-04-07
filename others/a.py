from pyspark.ml.feature import VectorAssembler
from pyspark.ml.linalg import Vectors
from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder \
    .appName("ElectricityUsageFeatures") \
    .getOrCreate()

# Sample data (You can replace this with your actual DataFrame)
data = [
    ("A1B2C3", "2024-09-29T10:00:00Z", 4.75, 230, 10.5, 1800, 8550, "268", "phuong_14", "quan_10"),
    ("D4E5F6", "2024-09-29T11:00:00Z", 3.25, 220, 9.8, 1800, 5850, "134", "phuong_7", "quan_3")
]

# Create DataFrame with the respective schema
columns = ["household_id", "timestamp", "electricity_usage_kwh", "voltage", "current", "price_per_kwh", "total_cost", "house_number", "ward", "district"]
df = spark.createDataFrame(data, columns)

# Assemble features from electricity_usage_kwh, voltage, and current into a single vector
assembler = VectorAssembler(inputCols=["electricity_usage_kwh", "voltage", "current"], outputCol="features")

# Transform the DataFrame to include the new "features" column
feature_df = assembler.transform(df)

# Show the DataFrame with features
feature_df.select("household_id", "features").show(truncate=False)

# Now you can use this feature_df to apply your machine learning model.
