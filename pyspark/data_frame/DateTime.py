from pyspark.sql import SparkSession
from pyspark.sql.functions import current_date, current_timestamp, date_add, datediff, year, month, dayofmonth, to_date, date_format

# Create SparkSession
spark = SparkSession.builder \
    .appName("Date Functions Example") \
    .getOrCreate()

# Sample data
data = [("2022-01-01",),
        ("2022-02-15",),
        ("2022-03-20",),
        ("2022-04-25",),
        ("2022-05-30",)]

# Define schema
schema = ["date"]

# Create DataFrame
df = spark.createDataFrame(data, schema)

# Show DataFrame
df.show()

# current_date() and current_timestamp()
df.withColumn("current_date", current_date()) \
  .withColumn("current_timestamp", current_timestamp()) \
  .show()
# date_add()
df.withColumn("date_add_10_days", date_add(df["date"], 10)) \
  .show()

# datediff()
df.withColumn("datediff_from_current", datediff(current_date(), df["date"])) \
  .show()

# year(), month(), dayofmonth()
df.withColumn("year", year(df["date"])) \
  .withColumn("month", month(df["date"])) \
  .withColumn("day", dayofmonth(df["date"])) \
  .show()

# to_date() and date_format()
df.withColumn("to_date", to_date(df["date"])) \
  .withColumn("date_format", date_format(df["date"], "MMM-dd-yyyy")) \
  .show()
