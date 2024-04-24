from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

# Create SparkSession
spark = SparkSession.builder \
    .appName("UnionExample") \
    .getOrCreate()

# Create sample DataFrame 1
data1 = [("John", 25), ("Alice", 30), ("Bob", 35)]
columns = ["Name", "Age"]
df1 = spark.createDataFrame(data1, columns)

# Create sample DataFrame 2
data2 = [("Tom", 22), ("Sarah", 27)]
df2 = spark.createDataFrame(data2, columns)

# Union
union_df = df1.union(df2)
union_all_df = df1.unionAll(df2)
union_by_name_df = df1.unionByName(df2)

# Output
print("Union:")
union_df.show()

print("Union All:")
union_all_df.show()

print("Union By Name:")
union_by_name_df.show()

# Stop SparkSession
spark.stop()
