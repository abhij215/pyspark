import findspark
findspark.init()

from pyspark import SparkContext


if __name__ == "__main__":

    sc = SparkContext("local[*]","search-data")

    inputFile = sc.textFile("file:///E:/abhishek/pyspark-projects/datasets/moviedata.data")
    ratings = inputFile.map(lambda x: (x.split("\t")[2],1))
    totalc_ratings = ratings.reduceByKey(lambda x,y: x+y).sortByKey(False)

    cou = totalc_ratings.collect()

    for a in cou:
        print(a)
