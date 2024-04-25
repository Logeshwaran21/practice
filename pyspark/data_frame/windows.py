from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Window Functions").getOrCreate()

data= [
    ("John", "HR", 100),
    ("Alice", "IT", 150),
    ("Bob", "Finance", 200),
    ("Emily", "Marketing", 250),
    ("David", "HR", 300),
    ("Sophia", "Finance", 350),
    ("James", "IT", 400),
    ("Emma", "Marketing", 450),
    ("Michael", "HR", 500),
    ("Olivia", "IT", 400),
    ("William", "Finance", 2000),
    ("Ava", "Marketing", 650),
    ("Alexander", "HR", 700),
    ("Mia", "IT", 750),
    ("Ethan", "Finance", 800),
    ("Liam", "Marketing", 850),
    ("Charlotte", "IT", 900),
    ("Benjamin", "Finance", 800),
    ("Isabella", "HR", 1000),
    ("Mason", "Marketing", 850)
]

schema = ["NAME","DEP","SALARY"]

df= spark.createDataFrame(data,schema)


#first we need create window functions
window= Window.partitionBy("DEP").orderBy("SALARY")


#rank window funtion it gives order wise rank and ignore the next value
rank_df = df.withColumn("rank",rank().over(window))
rank_df.show()

#dense_rank gives contineous of number in order with duplicate values with out skipping the next value
dense_rank = df.withColumn("dense_rank",dense_rank().over(window))
dense_rank.show()

#rownumber give the continuous number for all the groups
rownum_df = df.withColumn("row_number",row_number().over(window))
rownum_df.show()


#lead  gives the next value
lead_df = df.withColumn("LEAD",lead("SALARY",1).over(window))
lead_df.show()
#Lag gives the previous value
lag_df = df.withColumn("LAG",lag("SALARY",1).over(window))
lag_df.show()
