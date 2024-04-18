from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

my_conf = SparkConf()
my_conf.set("spark.app.name","pattern-searching")
my_conf.set("spark.master","local[*]")

spark = SparkSession.builder\
.config(conf=my_conf)\
.getOrCreate()

if __name__ == "__main__":

    myregex = r'^(\S+) (\S+)\t(\S+)\,(\S+)'

    inputfile = spark.read.csv("file:///E:/abhishek/pyspark-projects/datasets/order_new.dataset")

    filesnew = inputfile.select(regexp_extract('value',myregex,1).alias("order_id"),regexp_extract('value',myregex,2).alias("date"),regexp_extract('value',myregex,3).alias("customer_id"),regexp_extract('value',myregex,4).alias("status"))
    
    filesnew.printSchema()

    filesnew.show()

    filesnew.select("order_id").show()

    spark.stop()