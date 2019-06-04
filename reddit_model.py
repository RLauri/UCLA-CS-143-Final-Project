from __future__ import print_function
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
# TODO: Check to see if we need this
# from pyspark.sql.functions import udf
from pyspark.sql.types import StringType, ArrayType, BooleanType, FloatType
from pyspark.ml.feature import CountVectorizer
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.tuning import CrossValidator, CrossValidatorModel, ParamGridBuilder
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.sql.functions import ltrim, element_at
from py4j.protocol import Py4JJavaError
import os
import shutil

from cleantext import sanitize 


MIN_DF = 10
TRAIN = False


def modified_sanitize(text):
    """
    Function that takes the output of sanitize and returns the output desired
    by project 2b specs
    """
    _, unigrams, bigrams, trigrams = sanitize(text)
    ret = unigrams.split()
    ret.extend(bigrams.split())
    ret.extend(trigrams.split())
    # return unigrams + bigrams + trigrams
    return ret


def generate_joined_data(labeled_data, comments, context):
    """
    Creates a data frame containing all the joined comments from 
    labeled_data and comments
    """
    comments.createOrReplaceTempView("comments")
    labeled_data.createOrReplaceTempView("labeled_data")

    join_comment_sql = """
SELECT
    Input_id,
    labeldem,
    labelgop,
    labeldjt,
    comments.body AS body
FROM labeled_data
JOIN comments ON Input_id = id"""
    return context.sql(join_comment_sql)


def generate_full_comments_data(submissions, comments, context):
    """
    creates a data frame to fulfill the requirements of both tasks 8 and 9
    """

    # # needed submissions view and remakes comments view in case it hadn't been done previously
    submissions.createOrReplaceTempView("submissions")
    comments.createOrReplaceTempView("comments")

    full_comment_join_sql = """
SELECT
    comments.id AS comment_id,
    submissions.id AS submission_id,
    comments.body AS body,
    comments.created_utc AS timestamp,
    comments.author_flair_text AS state,
    submissions.title AS title,
    comments.score AS comment_score,
    submissions.score AS submission_score
FROM comments
JOIN submissions ON ltrim('t3_', comments.link_id) = submissions.id 
AND comments.body NOT LIKE '%/s%' 
AND comments.body NOT LIKE '&gt%'"""
    return context.sql(full_comment_join_sql)


def generate_sanitized_full_comments(context, full_comments_data):
    """
    takes full data and runs sanitize on body column
    """
    full_comments_data.createOrReplaceTempView("full_comments_data")
    sanitized_full_comments_sql = """
SELECT
    comment_id,
    submission_id,
    sanitize(body) AS body,
    timestamp,
    state,
    title, 
    comment_score,
    submission_score
FROM full_comments_data"""
    return context.sql(sanitized_full_comments_sql)


def main(context):
    """Main function takes a Spark SQL context."""
	# skips to task 10 if results from 9 are stored
    if os.path.isfile("full_sentiment_data.parquet/._SUCCESS.crc"):
        full_sentiment_data = context.read.parquet("full_sentiment_data.parquet")
    else:
        # TASK 1
        if os.path.isfile("comments.parquet/._SUCCESS.crc") and os.path.isfile("submissions.parquet/._SUCCESS.crc") and os.path.isfile("labeled_data.parquet/._SUCCESS.crc"):
            # print("WE HERE")
            comments = context.read.parquet("comments.parquet")
            submissions = context.read.parquet("submissions.parquet")
            labeled_data = context.read.parquet("labeled_data.parquet")
        else:
            comments = context.read.json("comments-minimal.json.bz2")
            comments.write.parquet("comments.parquet")

            submissions = context.read.json("submissions.json.bz2")
            submissions.write.parquet("submissions.parquet")

            labeled_data = context.read.csv("labeled_data.csv", header='true')
            labeled_data.write.parquet("labeled_data.parquet") 

        # Create temporary views for later
        comments.createGlobalTempView("comments")
        labeled_data.createGlobalTempView("labeled_data")
        submissions.createGlobalTempView("submissions")

        # TASK 4
        context.registerFunction("sanitize", modified_sanitize, ArrayType(StringType()))

        # TASK 5
        if os.path.isfile("joined_data.parquet/._SUCCESS.crc"):
            joined_data = context.read.parquet("joined_data.parquet")
        else:
            joined_data = generate_joined_data(labeled_data, comments, context)
            joined_data.write.parquet("joined_data.parquet")

        # code to run sanitize on joined data
        if os.path.isfile("ngrams.parquet/._SUCCESS.crc"):
            ngrams = context.read.parquet("ngrams.parquet")
        else:
            joined_data.createOrReplaceTempView("joined_data")

            ngram_sql = """
        SELECT 
            Input_id,
            labeldem,
            labelgop,
            labeldjt,
            sanitize(body) AS body
        FROM joined_data"""

            ngrams = context.sql(ngram_sql)
            ngrams.write.parquet("ngrams.parquet")

        # TASK 6A
        # REFERENCE:
        # https://spark.apache.org/docs/latest/ml-features.html#countvectorizer
        # couldn't figure out how to save this simply, so it always has to be ran? Ah well, was fast to run ¯\_(ツ)_/¯
        vectorizer = CountVectorizer(inputCol="body",
                                    outputCol="features",
                                    minDF=MIN_DF,
                                    binary=True)
        model = vectorizer.fit(ngrams)

        # TASK 6B
        if os.path.isdir("result.parquet"):
            result = context.read.parquet("result.parquet")
        else:
            result = model.transform(ngrams)
            result.write.parquet("result.parquet")
        

        if os.path.isdir("sentiment_data.parquet"):
            sentiment_data = context.read.parquet("sentiment_data.parquet")
        else:
            djt_sentiment_sql = """
        SELECT
            *,
            if (labeldjt = 1, 1, 0) AS pos_label,
            if (labeldjt = -1, 1, 0) AS neg_label
        FROM result"""
            result.createOrReplaceTempView("result")
            sentiment_data = context.sql(djt_sentiment_sql)
            sentiment_data.write.parquet("sentiment_data.parquet")
            # sentiment_data.show()

        # TASK 7
        if os.path.isfile("project2/pos.model/bestModel/data/._SUCCESS.crc") and os.path.isfile("project2/neg.model/bestModel/data/._SUCCESS.crc"):
            pos_model = CrossValidatorModel.load("project2/pos.model")
            neg_model = CrossValidatorModel.load("project2/neg.model")
        else:
            # Initialize two logistic regression models.
            # Replace labelCol with the column containing the label, and featuresCol with the column containing the features.
            poslr = LogisticRegression(labelCol="pos_label", featuresCol="features", maxIter=10)
            neglr = LogisticRegression(labelCol="neg_label", featuresCol="features", maxIter=10)
            # This is a binary classifier so we need an evaluator that knows how to deal with binary classifiers.
            posEvaluator = BinaryClassificationEvaluator(labelCol="pos_label")
            negEvaluator = BinaryClassificationEvaluator(labelCol="neg_label")
            # There are a few parameters associated with logistic regression. We do not know what they are a priori.
            # We do a grid search to find the best parameters. We can replace [1.0] with a list of values to try.
            # We will assume the parameter is 1.0. Grid search takes forever.
            posParamGrid = ParamGridBuilder().addGrid(poslr.regParam, [1.0]).build()
            negParamGrid = ParamGridBuilder().addGrid(neglr.regParam, [1.0]).build()
            # We initialize a 5 fold cross-validation pipeline.
            posCrossval = CrossValidator(
                estimator=poslr,
                evaluator=posEvaluator,
                estimatorParamMaps=posParamGrid,
                numFolds=5)
            negCrossval = CrossValidator(
                estimator=neglr,
                evaluator=negEvaluator,
                estimatorParamMaps=negParamGrid,
                numFolds=5)
            # Although crossvalidation creates its own train/test sets for
            # tuning, we still need a labeled test set, because it is not
            # accessible from the crossvalidator (argh!)
            # Split the data 50/50
            posTrain, posTest = sentiment_data.randomSplit([0.5, 0.5])
            negTrain, negTest = sentiment_data.randomSplit([0.5, 0.5])
            # Train the models
            print("Training positive classifier...")
            pos_model = posCrossval.fit(posTrain)
            print("Training negative classifier...")
            neg_model = negCrossval.fit(negTrain)

            # Once we train the models, we don't want to do it again. We can save the models and load them again later.
            pos_model.save("project2/pos.model")
            neg_model.save("project2/neg.model")

        # TASK 8
        # had to downsample because of RAM issues, seemed like the correct place to do it albeit a little redundant reloading
        # (Windows ate up way too much RAM on the desktop we used, and both of our laptops are too much of potatoes to run any of this)
        comments = context.read.parquet("comments.parquet").sample(False, 0.2 , None)
        submissions = context.read.parquet("submissions.parquet").sample(False, 0.2 , None)

        comments.createOrReplaceTempView("comments")
        submissions.createOrReplaceTempView("submissions")

        full_comments_data = generate_full_comments_data(submissions, comments, context)
        # full_comments_data.filter("state is not null").show()

        # TASK 9

        # task 4 redone
        # reregisters function in case of exception not happening for previous task 4
        context.registerFunction("sanitize", modified_sanitize, ArrayType(StringType()))

        # task 5 redone
        sanitized_full_comments = generate_sanitized_full_comments(context, full_comments_data)
        # sanitized_full_comments.show()

        # task 6A
        result_full_data = model.transform(sanitized_full_comments)
        # result_full_data.show()

        # classification part of task 9
        pos_result = pos_model.transform(result_full_data)
        # pos_result.show()
        neg_result = neg_model.transform(result_full_data)
        # neg_result.show()

        # probability threshold application from task 9
        context.registerFunction("first_element", lambda x: float(x[1]), FloatType())
        threshold_sql = """
    SELECT
        a.comment_id AS comment_id,
        a.submission_id AS submission_id,
        a.timestamp AS timestamp,
        a.state AS state,
        a.title AS title,
        if (first_element(a.probability) > 0.2, 1, 0) AS pos,
        if (first_element(b.probability) > 0.25, 1, 0) AS neg,
        a.comment_score AS comment_score,
        a.submission_score AS submission_score
    FROM pos_result a
    INNER JOIN neg_result b ON a.comment_id = b.comment_id
    """
        pos_result.createOrReplaceTempView("pos_result")
        neg_result.createOrReplaceTempView("neg_result")
        # pos_result.printSchema()
        # full_sentiment_data = context.sql(threshold_sql).explain()
        full_sentiment_data = context.sql(threshold_sql)
        full_sentiment_data.write.parquet("full_sentiment_data.parquet")
        # full_sentiment_data.show()
        full_sentiment_data.show(20, False)
        # exit(1)

    # TASK 10

    # part 1
    percent_sql = """
SELECT
    AVG(pos) * 100.0 AS Positive,
    AVG(neg) * 100.0 AS Negative
FROM full_sentiment_data"""
    full_sentiment_data.createOrReplaceTempView("full_sentiment_data")
    task10_1 = context.sql(percent_sql)
    # task10_1.show()
    if os.path.isdir("raw_percentages.csv"):
        shutil.rmtree("raw_percentages.csv")
    task10_1.repartition(1).write.format("com.databricks.spark.csv").option("header", "true").save("raw_percentages.csv")
    
    # part 2
    percent_by_day_sql = """
SELECT
    FROM_UNIXTIME(timestamp, 'YYYY-MM-dd') AS date,
    AVG(pos) * 100.0 AS Positive,
    AVG(neg) * 100.0 AS Negative
FROM full_sentiment_data
GROUP BY date
ORDER BY date"""
    # full_sentiment_data.createOrReplaceTempView("full_sentiment_data")
    task10_2 = context.sql(percent_by_day_sql)
    # task10_2.show()
    if os.path.isdir("time_data.csv"):
        shutil.rmtree("time_data.csv")
    task10_2.repartition(1).write.format("com.databricks.spark.csv").option("header", "true").save("time_data.csv")

    # part 4


if __name__ == "__main__":
    conf = SparkConf().setAppName("CS143 Project 2B")
    conf = conf.setMaster("local[*]")
    sc   = SparkContext(conf=conf)
    sqlContext = SQLContext(sc)
    sc.addPyFile("cleantext.py")
    main(sqlContext)
