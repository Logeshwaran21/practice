from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# Create a SparkSession
spark = SparkSession.builder.appName("Numeric Functions Example").getOrCreate()

# Sample data
data = [(1, 10.5), (2, -5.3), (3, 7.8), (4, -2.6), (5, 15.2)]
df = spark.createDataFrame(data, ["id", "value"])

# Apply numeric functions
df_sum = df.select(sum("value").alias("sum_value"))
df_avg = df.select(avg("value").alias("avg_value"))
df_min = df.select(min("value").alias("min_value"))
df_max = df.select(max("value").alias("max_value"))
df_round = df.select(round("value", 1).alias("rounded_value"))
df_abs = df.select(abs("value").alias("abs_value"))

# Show results
df_sum.show()
df_avg.show()
df_min.show()
df_max.show()
df_round.show()
df_abs.show()

# Stop SparkSession
spark.stop()
