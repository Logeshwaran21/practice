from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# create a spark session
spark= SparkSession.builder.appName("Aggregate functions")\
    .getOrCreate()

# create sample data
data = [("John", 25),("Alice", 30),("Bob", 35),("Alice", 28),("Bob", 40),("John", 22),("logesh waran",23)]
schema = StructType([StructField(name="NAME", dataType=StringType()),
                     StructField(name="AGE", dataType=IntegerType())])

#create dataframe
df= spark.createDataFrame(data,schema)
df.show()

# To find Mean value
mean_age=df.agg(mean("AGE").alias("Mean_age"))
mean_age.show()

#Average_age
average_age= df.agg(avg("AGE").alias("Average_age"))
average_age.show()

#To collect name list and return all the values in the form list
name_list= df.agg(collect_list("NAME").alias("Name_list"))
name_list.show()

# to collecct all the distinct values in a column
Name_set = df.agg(collect_set("NAME").alias("Name_set"))
Name_set.show()

#To count distinct values of the record
Distinct_Name= df.agg(countDistinct("NAME").alias("Distinct_Name_count"))
Distinct_Name.show()

#To count all records
Count_all=df.agg(count("NAME").alias("Count_of_all"))
Count_all.show()


#to show the first value and last value
First_name= df.agg(first("Name").alias("First_name"))
last_name= df.agg(last("Name").alias("Last_name"))
First_name.show()
last_name.show()