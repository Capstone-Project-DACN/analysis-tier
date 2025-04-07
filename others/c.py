from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.sql.functions import col
import pandas as pd
from scipy import stats

# Initialize Spark session
spark = SparkSession.builder.appName("ElectricityUsagePrediction").getOrCreate()

# Load data
data = spark.read.csv("household_power_consumption.txt", sep=";", header=True, inferSchema=True)

# Data processing
data = data.withColumn("Voltage", col("Voltage").cast("float")) \
           .withColumn("Global_intensity", col("Global_intensity").cast("float")) \
           .withColumn("Global_active_power", col("Global_active_power").cast("float")) \
           .withColumn("Sub_metering_1", col("Sub_metering_1").cast("float")) \
           .withColumn("Sub_metering_2", col("Sub_metering_2").cast("float")) \
           .withColumn("Sub_metering_3", col("Sub_metering_3").cast("float"))

data = data.dropna()

assembler = VectorAssembler(
    inputCols=["Voltage", "Global_intensity", "Sub_metering_1", "Sub_metering_2", "Sub_metering_3"],
    outputCol="features"
)
data = assembler.transform(data)

dataset = data.select("features", "Global_active_power")
train_data, test_data = dataset.randomSplit([0.8, 0.2], seed=42)

lr = LinearRegression(labelCol="Global_active_power", featuresCol="features")
lr_model = lr.fit(train_data)

predictions = lr_model.transform(test_data)
residuals = predictions.withColumn("residual", col("Global_active_power") - col("prediction"))

residuals_pd = residuals.select("residual").toPandas()

stat, p_value = stats.shapiro(residuals_pd['residual'])

print("Shapiro-Wilk Test Statistic:", stat)
print("p-value:", p_value)

# Stop Spark session
spark.stop()
