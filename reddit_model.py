from __future__ import print_function
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext


def main(context):
    """Main function takes a Spark SQL context."""
    context.read.parquet("comments.parquet").printSchema()

if __name__ == "__main__":
    conf = SparkConf().setAppName("CS143 Project 2B")
    conf = conf.setMaster("local[*]")
    sc   = SparkContext(conf=conf)
    sqlContext = SQLContext(sc)
    sc.addPyFile("cleantext.py")
    main(sqlContext)
