from pyspark import SparkConf

from pyspark.sql import SparkSession

my_conf = SparkConf()
my_conf.set("spark.app.name","orders-group-by")
my_conf.set("spark.master","local[*]")

spark = SparkSession.builder \
.config(conf=my_conf) \
.getOrCreate()

if __name__ == "__main__":

    orderrdf = spark.read. \
    option("header", True) \
    .option("inferSchema", True) \
    .csv("file:///E:/abhishek/pyspark-projects/datasets/orders-201019-002101.csv")

    #orderrdf.show()

    groupedOrder = orderrdf \
    .where("order_customer_id > 10000") \
    .select("order_id", "order_customer_id") \
    .groupBy("order_customer_id") \
    .count()

    groupedOrder.show()




    spark.stop()