import findspark
findspark.init()

from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

myConf = SparkConf()
myConf.set("spark.app.name","toDF")
myConf.set("spark.master","local[*]")

spark = SparkSession.builder\
.config(conf=myConf)\
.getOrCreate()


if __name__ == "__main__":

    myList = [(1,"2013-07-25",1159,"CLOSED"),
              (2,"2013-07-25",256,"CLOSED"),
              (3,"2013-07-25",17889,"CLOSED"),
              (4,"2013-07-25",1899,"CLOSED"),
              (5,"2013-07-25",1789,"CLOSED"),
              (6,"2013-07-25",12149,"CLOSED"),]
    

    dataList = spark.createDataFrame(myList)\
    .toDF("orderid","orderDate","customerid","status")

    