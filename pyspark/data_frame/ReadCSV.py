from pyspark.shell import spark
from pyspark.sql.types import *


data1= spark.read.csv(path="C:/Users/Logeshwaran A/Desktop/pyspark_practice/practice/pyspark/resource/onlinefoods.csv",header=True)
data1.show()
data1.printSchema()



#changing the shema on top of the data frame
schema= StructType([StructField(name="Age",dataType=IntegerType())])
data1= spark.read.csv(path="C:/Users/Logeshwaran A/Desktop/pyspark_practice/practice/pyspark/resource/onlinefoods.csv",header=True,schema=schema)
data1.show()
data1.printSchema()
