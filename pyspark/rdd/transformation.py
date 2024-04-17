from pyspark import SparkContext
sc= SparkContext.getOrCreate()

# map transformation
rdd_data=[1,2,3,4,5]
rdd_= sc.parallelize(rdd_data)
rdd=rdd_.map(lambda x: x*2)
print(rdd.collect())

#filter transformation
list2=[1,2,3,4,5,6,7,8,9]
rdd_list2= sc.parallelize(list2)
f_rdd= rdd_list2.filter(lambda x: x%2==0)
print(f_rdd.collect())

#union transformation
input=sc.parallelize([1,2,3,4,5,6,7,8,9])
u_rdd1= input.filter(lambda x:x%2==0)
u_rdd2= input.filter(lambda x:x%7==0)
print(u_rdd1.union(u_rdd2).collect())

#flatmap
rdd = sc.parallelize([1, 2, 3])
flat_rdd = rdd.flatMap(lambda x: (x, x * 10))
print(flat_rdd.collect())

rdd_fm= sc.parallelize(["hi logeshwaran","welcome to diggibyte"])
fm_rdd=rdd_fm.flatMap(lambda x: x.split(" "))
print(fm_rdd.collect())

#map partition
rdd = sc.parallelize([1, 2, 3, 4, 5], 3)
def add_one(iterator):
    return map(lambda x: x + 1, iterator)
mapped_rdd = rdd.mapPartitions(add_one)
print(mapped_rdd.collect())

#mappartitionsWithIndex
rdd = sc.parallelize([1, 2, 3, 4, 5], 2)
def add_partition_index(partition_index, iterator):
    return map(lambda x: (partition_index, x), iterator)
mapped_with_index_rdd = rdd.mapPartitionsWithIndex(add_partition_index)
print(mapped_with_index_rdd.collect())

#glom
rdd = sc.parallelize([1, 2, 3, 4, 5], 3)
glom_rdd = rdd.glom().collect()
print(glom_rdd)

#intersection
rdd1 = sc.parallelize([1, 2, 3])
rdd2 = sc.parallelize([2, 3, 4])
intersection_rdd = rdd1.intersection(rdd2)
print(intersection_rdd.collect())


#distinct
rdd = sc.parallelize([1, 1, 2, 3, 3])
distinct_rdd = rdd.distinct()
print(distinct_rdd.collect())

#groupbyKey
rdd = sc.parallelize([(1, 'a'), (1, 'b'), (2, 'c')])
grouped_rdd = rdd.groupByKey().mapValues(list)
print(grouped_rdd.collect())