from pyspark.shell import spark

df = spark.read.parquet("C:/Users/Logeshwaran A/Desktop/pyspark_practice/practice/pyspark/resource/part-r-00000-1a9822ba-b8fb-4d8e-844a-ea30d0801b9e.gz.parquet")
df.show()
df.printSchema()