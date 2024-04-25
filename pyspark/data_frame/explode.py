from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder \
    .appName("Explode Example") \
    .getOrCreate()

data = [(1, [1, 2, 3]), (2, [4, 5])]
df = spark.createDataFrame(data, ["id", "numbers"])

exploded_df = df.select("id", explode("numbers").alias("number"))
exploded_df.show()

data = [(1, [1, 2, 3]), (2, None)]
df = spark.createDataFrame(data, ["id", "numbers"])

exploded_df = df.select("id", explode_outer("numbers").alias("number"))
exploded_df.show()

data = [(1, ["a", "b", "c"]), (2, ["d", "e"])]
df = spark.createDataFrame(data, ["id", "letters"])

exploded_df = df.select("id", posexplode("letters").alias("pos", "letter"))
exploded_df.show()

data = [(1, ["a", "b", "c"]), (2, None)]
df = spark.createDataFrame(data, ["id", "letters"])

exploded_df = df.select("id", posexplode_outer("letters").alias("pos", "letter"))
exploded_df.show()

from pyspark.sql.functions import from_json, to_json
from pyspark.sql.types import StructType, StructField, StringType

# Define schema for JSON string
schema = StructType([
    StructField("name", StringType()),
    StructField("age", StringType())
])

# Example JSON string
json_string = '{"name": "John", "age": "30"}'

# Create DataFrame
df = spark.createDataFrame([(1, json_string)], ["id", "json_string"])

# Convert JSON string to struct
struct_df = df.withColumn("data", from_json("json_string", schema))

# Show DataFrame
struct_df.show(truncate=False)

# Convert struct to JSON string
json_df = struct_df.withColumn("json_string", to_json("data"))

# Show DataFrame
json_df.show(truncate=False)
