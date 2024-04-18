from pyspark import SparkContext
sc=SparkContext.getOrCreate()

# creating narrow transformation using map function
list1= [12,3,4,35,24,32]
rdd_list_1=sc.parallelize(list1)
map_func= rdd_list_1.map(lambda x:x*2)
print(map_func.collect())

#creating wide transformation using groupbyKey

list2= [(1,'a'),(2,'b'),(1,'c'),(2,'d'),(3,'e')]
rdd_list_2= sc.parallelize(list2)
rdd_gk=rdd_list_2.groupByKey().mapValues(list)
print(rdd_gk.collect())