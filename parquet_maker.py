from __future__ import print_function
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext


def main(context):
    """Main function takes a Spark SQL context."""
    # YOUR CODE HERE
    # YOU MAY ADD OTHER FUNCTIONS AS NEEDED

if __name__ == "__main__":
    conf = SparkConf().setAppName("CS143 Project 2B")
    conf = conf.setMaster("local[*]")
    sc   = SparkContext(conf=conf)
    sqlContext = SQLContext(sc)
    sc.addPyFile("cleantext.py")
    comments = sqlContext.read.json("comments-minimal.json.bz2")
    #submissions = sqlContext.read.json("submissions.json.bz2")
    #labeled_data = sqlContext.read.csv("labeled_data.csv", header='true')
    comments.write.parquet("comments.parquet")
    #submissions.write.parquet("submissions.parquet")
#labeled_data.write.parquet("labeled_data.parquet")
    main(sqlContext)
