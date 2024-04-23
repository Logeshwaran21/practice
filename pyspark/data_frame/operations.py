from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# Create a SparkSession
spark = SparkSession.builder \
    .appName("LogicalOperationsExample") \
    .getOrCreate()

# Create a sample DataFrame
data = [("Alice", 25, "F"),
        ("Bob", 30, "M"),
        ("Charlie", 35, "M"),
        ("Diana", 40, "F")]

columns = ["Name", "Age", "Gender"]
df = spark.createDataFrame(data, columns)

# Example using AND operation
# Selecting rows where Age is greater than 30 AND Gender is 'M'
result_and = df.filter(expr("(Age > 30) AND (Gender = 'M')"))

# Example using OR operation
# Selecting rows where Age is greater than 30 OR Gender is 'F'
result_or = df.filter(expr("(Age > 30) OR (Gender = 'F')"))

# Show the results
print("Results using AND operation:")
result_and.show()

print("Results using OR operation:")
result_or.show()

# Stop the SparkSession
spark.stop()
