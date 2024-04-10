from pyspark import SparkConf
from pyspark.sql import SparkSession

my_conf = SparkConf()
my_conf.set("spark.app.name","dataframes-code")
my_conf.set("spark.master","local[*]")

spark = SparkSession.builder.config(conf=my_conf).getOrCreate()

if __name__ == "__main__":
    OrdersDf = spark.read.csv("file:///E:/abhishek/pyspark-projects/datasets/airports.csv")
    
    OrdersDf.show()
    
    spark.stop()