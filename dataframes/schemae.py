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
        StructField("order_id", IntegerType()),
        StructField("order_date", TimestampType()),
        StructField("order_customer_id", IntegerType()),
        StructField("order_status", StringType())
    ])

    # explicit creating using DDL
    OrderDDLSchema = """order_id Integer, 
                        order_date Timestamp, 
                        order_customer_id Integer,
                        order_status String"""

    orderrdf = spark.read \
    .format("csv") \
    .option("header", True) \
    .schema(OrderDDLSchema) \
    .option("path","file:///E:/abhishek/pyspark-projects/datasets/orders-201019-002101.csv") \
    .load()

    orderrdf.show()

    OrderDf = orderrdf.repartition(4)

    OrderDf.write \
    .format("csv") \
    .mode("overwrite") \
    .option("path","file:///E:/abhishek/pyspark-projects/sink/orderdf") \
    .save()

    spark.stop()