import findspark
findspark.init()

from pyspark import SparkContext

sc = SparkContext("local[*]","word-count")
input = sc.textFile("file:///E:/abhishek/learning/big-data/datasets/new.txt")
flatted = input.flatMap(lambda x: x.split(" "))
mapped = flatted.map(lambda x: (x,1))
co = mapped.reduceByKey(lambda x,y: (x+y))
sd = co.collect()

for a in sd:
    print(a)