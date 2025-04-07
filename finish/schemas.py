from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType, TimestampType

# Define schema for the household data
household_schema = StructType([
    StructField("household_id", StringType(), nullable=False),
    StructField("device_id", StringType(), nullable=False),
    StructField("timestamp", TimestampType(), nullable=False),
    StructField("electricity_usage_kwh", FloatType(), nullable=False),
    StructField("voltage", IntegerType(), nullable=False),
    StructField("current", FloatType(), nullable=False),
    StructField("location", StructType([
        StructField("house_number", StringType(), nullable=False),
        StructField("ward", StringType(), nullable=False),
        StructField("district", StringType(), nullable=False),
        StructField("city", StringType(), nullable=False)
    ]), nullable=False),
    StructField("price_per_kwh", IntegerType(), nullable=False),
    StructField("total_cost", IntegerType(), nullable=False)
])

# Define schema for the area data
area_schema = StructType([
    StructField("type", StringType(), nullable=False),
    StructField("device_id", StringType(), nullable=False),
    StructField("timestamp", TimestampType(), nullable=False),
    StructField("ward", StringType(), nullable=False),
    StructField("district", StringType(), nullable=False),
    StructField("city", StringType(), nullable=False),
    StructField("total_electricity_usage_kwh", FloatType(), nullable=False),
])