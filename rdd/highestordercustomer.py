import findspark
findspark.init()

from pyspark import SparkContext
sc = SparkContext("local[*]","customer-with-biggest-order")

if __name__ == "__main__":
    inputFile = sc.textFile("file:///E:/abhishek/pyspark-projects/datasets/customerorders.csv")
    indi = inputFile.map(lambda x: (x.split(",")[0], float(x.split(",")[2])))
    cou = indi.reduceByKey(lambda x,y: x+y)
    highest = cou.sortBy(lambda x:x[1], False)

    print(highest.collect()[0][0:])