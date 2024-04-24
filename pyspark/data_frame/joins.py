from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast

# Creating a SparkSession
spark = SparkSession.builder \
    .appName("join_examples") \
    .getOrCreate()

# Creating sample DataFrames
data1 = [("Alice", 34), ("Bob", 45), ("Charlie", 27)]
data2 = [("Alice", "Engineer"), ("Bob", "Doctor"), ("David", "Artist")]

df1 = spark.createDataFrame(data1, ["Name", "Age"])
df2 = spark.createDataFrame(data2, ["Name", "Profession"])

# Displaying DataFrames
print("DataFrame 1:")
df1.show()

print("DataFrame 2:")
df2.show()

inner_join = df1.join(df2, "Name", "inner")
inner_join.show()

cross_join = df1.crossJoin(df2)
cross_join.show()

outer_join = df1.join(df2, "Name", "outer")
outer_join.show()

left_join = df1.join(df2, "Name", "left")
left_join.show()

right_join = df1.join(df2, "Name", "right")
right_join.show()

left_semi_join = df1.join(df2, "Name", "leftsemi")
left_semi_join.show()

left_anti_join = df1.join(df2, "Name", "leftanti")
left_anti_join.show()

self_join = df1.alias("df1").join(df1.alias("df2"), "Name")
self_join.show()


broadcast_join = df1.join(broadcast(df2), "Name")
broadcast_join.show()

