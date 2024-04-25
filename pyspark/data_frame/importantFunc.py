from pyspark.sql.functions import *
from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder \
    .appName("UDF Example") \
    .getOrCreate()

# Sample DataFrame
data = [(1,), (2,), (3,), (4,)]
df = spark.createDataFrame(data, ["numbers"])

#UDF- user defined function

def square_udf(num):
    return num**2
df = df.withColumn("Square_UDF",square_udf(df["numbers"]))
df.show()


# Sample DataFrame
data_pivot = [("Product_A", "2019-01-01", 100),
        ("Product_B", "2019-01-01", 150),
        ("Product_A", "2019-01-02", 120),
        ("Product_B", "2019-01-02", 200)]
df_pivot = spark.createDataFrame(data_pivot, ["product", "date", "sales"])

# Pivot DataFrame
pivot_df = df_pivot.groupBy("date").pivot("product").sum("sales")

# Show pivoted DataFrame
pivot_df.show()

# Sample pivoted DataFrame
pivot_data = [("2019-01-01", 100, 150),
              ("2019-01-02", 120, 200)]
pivot_df = spark.createDataFrame(pivot_data, ["date", "Product_A", "Product_B"])

# Unpivot DataFrame
unpivot_df = pivot_df.selectExpr("date", "stack(2, 'Product_A', Product_A, 'Product_B', Product_B) as (product, sales)")

# Show unpivoted DataFrame
unpivot_df.show()


#transform
def double_values(x,i): #x= values inside the column, i = index position
    return when(i%2==0, x).over(-x)
data_transform=[[(1,[1,2,3,4,5])]]
schema=["id","values"]
df_trans= spark.createDataFrame(data_transform,schema)
df_trans.withColumn("transform",transform("values",double_values))
df_trans.show()

def alternate(x, i):
    return when(i % 2 == 0, x).otherwise(-x)
df = spark.createDataFrame([(1, [1, 2, 3, 4])], ("key", "values"))
df.withColumn("alternated",transform("values", alternate)).show()





