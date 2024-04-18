from pyspark.shell import spark



data1= spark.read.json(path="C:/Users/Logeshwaran A/Desktop/pyspark_practice/practice/pyspark/resource/iris.json")
data1.show()
data1.printSchema()



