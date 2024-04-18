from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# Create a SparkSession
spark = SparkSession.builder \
    .appName("StringFunctionsExample") \
    .getOrCreate()

# Sample input data
data = [("   Hello   ",),
        ("   World   ",),
        ("   PySpark   ",)]

# Create a DataFrame
df = spark.createDataFrame(data, ["input_string"])

# upper() - Convert string to uppercase
df = df.withColumn("output_upper", upper(col("input_string")))

# trim() - Remove leading and trailing whitespaces
df = df.withColumn("output_trim", trim(col("input_string")))

# ltrim() - Remove leading whitespaces
df = df.withColumn("output_ltrim", ltrim(col("input_string")))

# rtrim() - Remove trailing whitespaces
df = df.withColumn("output_rtrim", rtrim(col("input_string")))

# translate() - Replace characters in a string
df = df.withColumn("output_translate", translate(col("input_string"), "o", "7"))

# substring_index() - Extract substring before the nth occurrence of a delimiter
df = df.withColumn("output_substring_index", substring_index(col("input_string"), " ", 1))

# substring() - Extract substring from a string
df = df.withColumn("output_substring", substring(col("input_string"), 3, 5))

# split() - Split a string based on a delimiter
df = df.withColumn("output_split", split(col("input_string"), " "))

# repeat() - Repeat a string n times
df = df.withColumn("output_repeat", repeat(col("input_string"), 2))

# rpad() - Right-pad a string with spaces to a certain length
df = df.withColumn("output_rpad", rpad(col("input_string"), 15, " "))

# lpad() - Left-pad a string with spaces to a certain length
df = df.withColumn("output_lpad", lpad(col("input_string"), 15, " "))

# regex_replace() - Replace substrings matching a regex pattern
df = df.withColumn("output_regex_replace", regexp_replace(col("input_string"), "\s", "_"))

# Lower() - Convert string to lowercase
df = df.withColumn("output_lower", lower(col("input_string")))

# regex_extract() - Extract substrings matching a regex pattern
df = df.withColumn("output_regex_extract", regexp_extract(col("input_string"), "(\w+)", 1))

# length() - Get the length of a string
df = df.withColumn("output_length", length(col("input_string")))

# Show the DataFrame with input and output columns
df.show(truncate=False)

# Stop the SparkSession
spark.stop()
