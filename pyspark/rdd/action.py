from pyspark import SparkContext
sc=SparkContext.getOrCreate()
#collect
rdd = sc.parallelize([1, 2, 3])
print(rdd.collect())

#take
rdd = sc.parallelize([1, 2, 3, 4, 5])
taken = rdd.take(3)
print(taken)

#top
rdd = sc.parallelize([5, 3, 1, 2, 4])
top_elements = rdd.top(3)
print(top_elements)

#count
rdd = sc.parallelize([1, 2, 3, 4, 5])
count = rdd.count()
print(count)

#min()
rdd = sc.parallelize([5, 3, 1, 2, 4])
min_value = rdd.min()
print(min_value)

#max
rdd = sc.parallelize([5, 3, 1, 2, 4])
max_value = rdd.max()
print(max_value)

#mean
rdd = sc.parallelize([1, 2, 3, 4, 5])
mean_value = rdd.mean()
print(mean_value)

#reduce
rdd = sc.parallelize([1, 2, 3, 4, 5])
sum_value = rdd.reduce(lambda x, y: x + y)
print(sum_value)

#countByKey
rdd = sc.parallelize([(1, 'a'), (1, 'b'), (2, 'c'), (3, 'd'), (3, 'e')])
count_by_key = rdd.countByKey()
print(count_by_key)

#countbyvalue
rdd = sc.parallelize([1, 1, 2, 3, 3])
count_by_value = rdd.countByValue()
print(count_by_value)

#fold
rdd = sc.parallelize([1, 2, 3, 4, 5])
folded_value = rdd.fold(0, lambda x, y: x + y)
print(folded_value)
