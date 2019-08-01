from pyspark import SparkConf, SparkContext
conf = (SparkConf()
         .setAppName("MovieRating"))
sc1 = SparkContext(conf = conf)

