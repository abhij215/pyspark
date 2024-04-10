import findspark
findspark.init()

from pyspark import SparkContext
sc = SparkContext("local[*]","customer-with-biggest-order")

if __name__ == "__main__":
    inputFile = sc.textFile("file:///E:/abhishek/pyspark-projects/datasets/tempdata-201125-161348.csv")
    readings = inputFile.map(lambda x: (x.split(",")[0], x.split(",")[2], x.split(",")[3]))
    filtered  = readings.filter(lambda x: x[1] == "TMIN")
    temprr = filtered.map(lambda x: (x[0], x[2]))
    yell = temprr.reduceByKey(lambda x,y: min(x,y))
    cou = yell.collect()

    for a in cou:
        print(a)