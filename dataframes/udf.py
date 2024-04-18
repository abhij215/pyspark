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

    ageData = spark.read.format("csv")\
    .option("inferSchema",True)\
    .option("path","file:///E:/abhishek/pyspark-projects/datasets/age_df.dataset")\
    .load()

    def ageCheck(age):
        if age > 18:
            return 'Y'
        else:
            return 'N'

    ageDataS = ageData.toDF("Name","Age","City")

    #using column object expression - wont register in catalog
    #parseAgeFunction = udf(ageCheck, StringType())
    #ageDataC = ageDataS.withColumn("adult", parseAgeFunction("age"))
    #ageDataC.printSchema()
    #ageDataC.show()

    #using sql expression
    spark.udf.register("parseAgeFunction", ageCheck, StringType())
    ageDataSQL = ageDataS.withColumn("adult", expr("parseAgeFunction(age)"))
    ageDataSQL.show()