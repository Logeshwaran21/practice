from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# initialize the spark session
spark= SparkSession.builder.appName("DataFrame_Functions").getOrCreate()

#creating  sample dataframe
data = [
    (1, "Alice", 25),
    (2, "Bob", 30),
    (3, "Charlie", 35),
    (4, "David", 40),
    (5, "Emma", 45)
]
columns = ["ID", "Name", "Age"]
df = spark.createDataFrame(data, columns)

# count - no. of rows in dataframe
df_count= df.count()
print("no. of rows in Dataframe: ",df_count)

#select = selectiong the particular columns
df_selected= df.select("Name",'Age')
print("selected columns")
df_selected.show()


#filter &where = filter the columns base on conditions applies
df_filter= df.filter(df["Age"] > 30)
print("filtered rows: ")
df_filter.show()

#like
df_like= df.filter(df["Name"].like("A%"))
print("Name start with A")
df_like.show()

#describe
df_desc=df.describe()
df_desc.show()

# 6. columns()
column_list = df.columns
print("6. Column names:", column_list)

# 7. when() & otherwise()
df_with_condition = df.withColumn("AgeGroup", when(df["Age"] > 30, "Senior").otherwise("Young"))
print("7. DataFrame with conditional column:")
df_with_condition.show()

# 8. alias()
df_aliased = df.withColumnRenamed("Name", "FullName")
print("8. DataFrame with aliased column:")
df_aliased.show()

# 9. orderBy() & sort()
df_ordered = df.orderBy("Age", ascending=False)
print("9. DataFrame ordered by Age in descending order:")
df_ordered.show()

# 10. groupBy() & groupBy agg()
df_grouped = df.groupBy("Age").agg({"ID": "count"})
print("10. DataFrame grouped by Age with count:")
df_grouped.show()

# Stop the SparkSession
spark.stop()

