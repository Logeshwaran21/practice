import pandas as pd
from pyspark.sql import SparkSession

# Create SparkSession
spark = SparkSession.builder \
    .appName("DuplicateRemoval") \
    .getOrCreate()

# Sample DataFrame with duplicates
data = {'A': [1, 2, 2, 3, 3],
        'B': ['a', 'b', 'b', 'c', 'c']}
df = pd.DataFrame(data)

# Using drop_duplicates() to get distinct rows
distinct_df = df.drop_duplicates()
print(distinct_df)

# Sample DataFrame with duplicates
data2 = {'A': [1, 2, 2, 3, 3],
        'B': ['a', 'b', 'b', 'c', 'c']}
df2 = spark.createDataFrame(pd.DataFrame(data2))

# Removing duplicates using dropDuplicates()
distinct_df2 = df2.dropDuplicates()
distinct_df2.show()

# Removing duplicates based on specific columns
distinct_df2 = df.drop_duplicates(subset=['A'])
distinct_df2.show()

# Removing duplicates using multiple columns
distinct_df = df.drop_duplicates(subset=['A', 'B'])
distinct_df.show()