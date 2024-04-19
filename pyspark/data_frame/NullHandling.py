#Filtering Nulls
from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder \
    .appName("Handling NULL Values") \
    .getOrCreate()

# Sample DataFrame
data = [(1, "Alice", None),
        (2, None, 25),
        (3, "Bob", 30),
        (4, "Charlie", 35)]

columns = ["ID", "Name", "Age"]
df = spark.createDataFrame(data, columns)

# Filter NULL values in any column
filtered_df = df.dropna()

# Show filtered DataFrame
filtered_df.show()

#2.Replacing Nulls
# Replace NULL values with a specified value
filled_df = df.fillna({"Name": "Unknown", "Age": 0})

# Show DataFrame after filling NULL values
filled_df.show()

#3. Using Coalesce
from pyspark.sql.functions import coalesce

# Create a new column with non-NULL values
coalesced_df = df.withColumn("NewName", coalesce(df["Name"], df["Age"]))

# Show DataFrame with coalesced column
coalesced_df.show()

#4.Drop rows with any NULL values
dropped_df = df.na.drop()

# Show DataFrame after dropping NULL values
dropped_df.show()

#fillna() - Filling NULL Values with Specified Values
#Fill NULL values in specified columns
filled_df = df.fillna({"Name": "Unknown", "Age": 0})

# Show DataFrame after filling NULL values
filled_df.show()

#5. fill() - Filling NULL Values with a Single Value
# Fill all NULL values in DataFrame with a single value
filled_df = df.fillna(0)  # Fill all NULL values with 0

# Show DataFrame after filling NULL values
filled_df.show()
