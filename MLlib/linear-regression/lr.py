from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.sql.functions import col
from pyspark.ml.evaluation import RegressionEvaluator

spark = SparkSession.builder.appName("ElectricityUsagePrediction").getOrCreate()

data = spark.read.csv("household_power_consumption.txt", sep=";", header=True, inferSchema=True)


assembler = VectorAssembler(
    inputCols=["Voltage", "Global_intensity", "Sub_metering_1", "Sub_metering_2", "Sub_metering_3"],
    outputCol="features"
)
data = data.withColumn("Voltage", col("Voltage").cast("float")) \
           .withColumn("Global_intensity", col("Global_intensity").cast("float")) \
           .withColumn("Global_active_power", col("Global_active_power").cast("float")) \
           .withColumn("Sub_metering_1", col("Sub_metering_1").cast("float")) \
           .withColumn("Sub_metering_2", col("Sub_metering_2").cast("float")) \
           .withColumn("Sub_metering_3", col("Sub_metering_3").cast("float"))

data = data.dropna(subset=["Voltage", "Global_intensity", "Sub_metering_1", "Sub_metering_2", "Sub_metering_3","Global_active_power"])
#y=x0 + a*x1 +b*x2
data = assembler.transform(data)

dataset = data.select("features", "Global_active_power")

train_data, test_data = dataset.randomSplit([0.7, 0.3], seed=42)

lr = LinearRegression(labelCol="Global_active_power", featuresCol="features")

lr_model = lr.fit(train_data)
 
predictions = lr_model.transform(test_data)

predictions.select("features", "Global_active_power", "prediction").show(truncate=False)

evaluator_r2 = RegressionEvaluator(labelCol="Global_active_power", predictionCol="prediction", metricName="r2")
evaluator_mse = RegressionEvaluator(labelCol="Global_active_power", predictionCol="prediction", metricName="mse")

r2 = evaluator_r2.evaluate(predictions)
mse = evaluator_mse.evaluate(predictions)

print(f"R²: {r2}")
print(f"MSE: {mse}")

spark.stop()
















# from pyspark.ml.stat import Correlation
# from pyspark.ml.feature import VectorAssembler
# from pyspark.sql import SparkSession
# from pyspark.sql.functions import col

# # Initialize Spark session
# spark = SparkSession.builder.appName("ElectricityUsagePrediction").getOrCreate()

# # Load the dataset
# data = spark.read.csv("household_power_consumption.txt", sep=";", header=True, inferSchema=True)

# # Cast columns to numeric types
# data = data.withColumn("Voltage", col("Voltage").cast("float")) \
#            .withColumn("Global_intensity", col("Global_intensity").cast("float")) \
#            .withColumn("Global_active_power", col("Global_active_power").cast("float"))\
#            .withColumn("Sub_metering_1", col("Sub_metering_1").cast("float")) \
#            .withColumn("Sub_metering_2", col("Sub_metering_2").cast("float")) \
#            .withColumn("Sub_metering_3", col("Sub_metering_3").cast("float")) \

# # Drop rows with nulls
# data = data.dropna()

# # Assemble features into a single vector column
# assembler = VectorAssembler(
#     inputCols=["Voltage", "Global_intensity", "Sub_metering_1", "Sub_metering_2", "Sub_metering_3"],
#     outputCol="features"
# )
# data = assembler.transform(data)

# # Compute the Pearson correlation matrix
# correlation_matrix = Correlation.corr(data, "features").head()
# print("Pearson correlation matrix:\n" + str(correlation_matrix[0]))
# correlations = {}

# # List of features to calculate correlation with Global_active_power
# features = ["Voltage", "Global_intensity", "Sub_metering_1", "Sub_metering_2", "Sub_metering_3"]

# # Calculate the correlation for each feature with the target
# for feature in features:
#     correlation = data.stat.corr(feature, "Global_active_power")
#     correlations[feature] = correlation

# # Convert to DataFrame for better visualization
# correlation_matrix = spark.createDataFrame(correlations.items(), ["Feature", "Correlation"])
# correlation_matrix.show()

# # Stop the Spark session
# spark.stop()
