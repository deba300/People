from pyspark import SparkConf, SparkContext
conf = (SparkConf()
         .setAppName("MovieRating"))
sc = SparkContext(conf = conf)

