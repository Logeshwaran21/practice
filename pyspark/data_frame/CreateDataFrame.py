from pyspark.shell import spark
from pyspark.sql.types import *

# created dataFrame with default datatypes
data = [(1,'logesh'),(2,'suba'),(3,'logeshsuba')]
col_name= ['id','name']
df= spark.createDataFrame(data,col_name)
df.show()
df.printSchema()

# created a dataFrame with defined data types
data_2= [(1,'logesh'),(2,'suba'),(3,'logeshsuba'),(4,'logeshwaran')]
col_name2=StructType([StructField(name='id',dataType=IntegerType()),
                      StructField(name='Name',dataType=StringType())])
df1= spark.createDataFrame(data_2,col_name2)
df1.show()
df1.printSchema()

#created dataframe with dictionary (lie json file)
# no need of schema key will act as col name
data_3=[{'id':1,'name':'logesh'},
        {'id':2,'name':'suba'},
        {'id':3,'name':'subasree'}]
df3=spark.createDataFrame(data_3)
df3.printSchema() #default datatypes
df3.show()