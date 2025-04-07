from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.sql.functions import col, to_date
from pyspark.ml.evaluation import RegressionEvaluator
import pyspark.pandas as ps

spark = SparkSession.builder.appName("ElectricityUsagePrediction").getOrCreate()

df = spark.read.csv("Electric_Production.csv", header=True, inferSchema=True)

df = df.withColumn("Date", to_date(col("Date"), "dd-MM-yyyy"))

# null_count_consumption = df.filter(col("Consumption").isNull()).count()
# print(f"Number of NA rows in 'Consumption' column: {null_count_consumption}")

# df=df.dropna()
# print(type(df))
# print(f"Number of NA rows in 'Consumption' column: {df.count()}")

temp_df = ps.DataFrame( df ).set_index('Date')

temp_df.Date.plot.pie()



# assembler = VectorAssembler(
#     inputCols=["Voltage", "Global_intensity", "Sub_metering_1", "Sub_metering_2", "Sub_metering_3"],
#     outputCol="features"
# )
# data = data.withColumn("Voltage", col("Voltage").cast("float")) \
#            .withColumn("Global_intensity", col("Global_intensity").cast("float")) \
#            .withColumn("Global_active_power", col("Global_active_power").cast("float")) \
#            .withColumn("Sub_metering_1", col("Sub_metering_1").cast("float")) \
#            .withColumn("Sub_metering_2", col("Sub_metering_2").cast("float")) \
#            .withColumn("Sub_metering_3", col("Sub_metering_3").cast("float"))

# data = data.dropna(subset=["Voltage", "Global_intensity", "Sub_metering_1", "Sub_metering_2", "Sub_metering_3","Global_active_power"])
# #y=x0 + a*x1 +b*x2
# data = assembler.transform(data)

# dataset = data.select("features", "Global_active_power")

# train_data, test_data = dataset.randomSplit([0.7, 0.3], seed=42)

# lr = LinearRegression(labelCol="Global_active_power", featuresCol="features")

# lr_model = lr.fit(train_data)
 
# predictions = lr_model.transform(test_data)

# predictions.select("features", "Global_active_power", "prediction").show(truncate=False)

# evaluator_r2 = RegressionEvaluator(labelCol="Global_active_power", predictionCol="prediction", metricName="r2")
# evaluator_mse = RegressionEvaluator(labelCol="Global_active_power", predictionCol="prediction", metricName="mse")

# r2 = evaluator_r2.evaluate(predictions)
# mse = evaluator_mse.evaluate(predictions)

# print(f"R²: {r2}")
# print(f"MSE: {mse}")

spark.stop()







