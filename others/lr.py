from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression

# Create Spark session
spark = SparkSession.builder.appName("LinearRegressionExample").getOrCreate()

# Sample data for electricity usage (hourly data with features such as voltage, current, etc.)
data = [
    ("A1B2C3", "2024-09-29T10:00:00Z", 4.75, 230, 10.5, 8550),
    ("A1B2C3", "2024-09-29T11:00:00Z", 5.1, 235, 10.7, 9200),
    ("A1B2C3", "2024-09-29T12:00:00Z", 4.9, 225, 10.1, 8700),
]

columns = ["household_id", "timestamp", "electricity_usage_kwh", "voltage", "current", "total_cost"]

df = spark.createDataFrame(data, columns)

# Feature selection
assembler = VectorAssembler(inputCols=["voltage", "current", "total_cost"], outputCol="features")
df = assembler.transform(df)
df = df.select("features", "electricity_usage_kwh")
# df.show()


# Split the data into training and test sets (80% training, 20% testing)
train_data, test_data = df.randomSplit([0.8, 0.2])

print(train_data)
print(test_data)

# lr = LinearRegression(featuresCol='features', labelCol='electricity_usage_kwh')

# lr_model = lr.fit(train_data)

# test_results = lr_model.evaluate(test_data)

# # print(f"RMSE: {test_results.rootMeanSquaredError}")
# # print(f"R2: {test_results.r2}")

# threshold = 5.0

# predictions = lr_model.transform(test_data)

# predictions = predictions.withColumn("excess_usage", (predictions["prediction"] > threshold).cast("integer"))

# predictions.select("features", "electricity_usage_kwh", "prediction", "excess_usage").show()