from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, TimestampType, StringType

my_conf = SparkConf()
my_conf.set("spark.app.name","explicit-schema")
my_conf.set("spark.master","local[*]")

spark = SparkSession.builder\
.config(conf=my_conf)\
.enableHiveSupport()\
.getOrCreate()


if __name__ == "__main__":

    orderrdf = spark.read\
    .format("csv")\
    .option("header", True)\
    .option("inferSchema", True)\
    .option("path","file:///E:/abhishek/pyspark-projects/datasets/orders-201019-002101.csv")\
    .load()

    #orderrdf.createOrReplaceTempView("orders")

    #spark.sql("select order_status , count(*) as total_orders from orders group by order_status").show()

    spark.sql("create database if not exists retails")

    orderrdf.write\
    .format("csv")\
    .saveAsTable("retails.order1")


    spark.stop()