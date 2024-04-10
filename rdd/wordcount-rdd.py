import findspark
findspark.init()

from pyspark import SparkContext

from sys import stdin

if __name__ == "__main__":
    sc = SparkContext("local[*]","word-count")
    sc.setLogLevel("ERROR")
    input = sc.textFile("file:///E:/abhishek/pyspark-projects/datasets/search-data.txt")
    flatted = input.flatMap(lambda x: x.split(" "))
    mapped = flatted.map(lambda x: (x,1))
    co = mapped.reduceByKey(lambda x,y: (x+y))
    sd = co.collect()

    for a in sd:
        print(a)

stdin.readline()