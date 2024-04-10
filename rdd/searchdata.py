import findspark
findspark.init()

from pyspark import SparkContext


if __name__ == "__main__":

    sc = SparkContext("local[*]","search-data")

    inputFile = sc.textFile("file:///E:/abhishek/pyspark-projects/datasets/search-data.txt")
    mapped = inputFile.flatMap(lambda x: x.split(" ")).map(lambda x: (x.lower(),1))
    rever = mapped.reduceByKey(lambda x,y: x+y).map(lambda x: (x[1],x[0]))
    totalco = rever.sortByKey(False).map(lambda x: (x[1],x[0]))

    result = totalco.collect()

    for i in result:
        print(i)
