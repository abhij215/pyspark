from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, TimestampType, StringType

my_conf = SparkConf()
my_conf.set("spark.app.name","explicit-schema")
my_conf.set("spark.master","local[*]")

spark = SparkSession.builder \
.config(conf=my_conf) \
.getOrCreate()


if __name__ == "__main__":

    # explicit creating schema using struct type
    orderSchema = StructType([
        StructField("Order_Id", IntegerType()),
        StructField("Order_Date", TimestampType()),
        StructField("Order_Customer_Id", IntegerType()),
        StructField("Order_Status", StringType())
    ])

    # explicit creating using DDL
    OrderDDLSchema = "OrderId Integer, OrderDate Timestamp, OrderCustomerId Integer, OrderStatus String"

    orderrdf = spark.read \
    .format("csv") \
    .schema(OrderDDLSchema) \
    .option("path","file:///E:/abhishek/pyspark-projects/datasets/orders-201019-002101.csv") \
    .load()

    OrderDf = orderrdf.repartition(4)

    OrderDf.write \
    .format("csv") \
    .mode("append") \
    .option("path","file:///E:/abhishek/pyspark-projects/sink/orderdf") \
    .save()

    spark.stop()