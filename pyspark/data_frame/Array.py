from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("Array Functions Example") \
    .getOrCreate()

# Sample data
data = [("John", ["apple", "banana", "orange"]),
        ("Alice", ["grape", "banana", "kiwi"]),
        ("Bob", ["apple", "orange", "mango"])]

# Create DataFrame
df = spark.createDataFrame(data, ["Name", "Fruits"])

# Show initial DataFrame
print("Initial DataFrame:")
df.show()

# ARRAY function: Create an array
df = df.withColumn("New_Fruits", array("Fruits"))
print("\nDataFrame after ARRAY function:")
df.show()

# ARRAY_CONTAINS function: Check if an array contains a specific element
df = df.withColumn("Contains_Orange", array_contains("Fruits", "orange"))
print("\nDataFrame after ARRAY_CONTAINS function:")
df.show()

# ARRAY_LENGTH function: Get the length of an array
df = df.withColumn("Fruits_Count", size("Fruits"))
print("\nDataFrame after ARRAY_LENGTH function:")
df.show()

# ARRAY_POSITION function: Get the position of an element in an array
df_position = df.withColumn("Array_position",array_position("Fruits",("banana"))).show()

# ARRAY_REMOVE function: Remove an element from an array
df_remov = df.withColumn("Fruits_without_apple", array_remove("Fruits", "apple"))
print("\nDataFrame after ARRAY_REMOVE function:")
df_remov.show()

# Stop SparkSession
spark.stop()
