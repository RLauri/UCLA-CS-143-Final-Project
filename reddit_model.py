from __future__ import print_function
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
# TODO: Check to see if we need this
# from pyspark.sql.functions import udf
from pyspark.sql.types import StringType, ArrayType, BooleanType
from pyspark.ml.feature import CountVectorizer
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.ml.evaluation import BinaryClassificationEvaluator


from cleantext import sanitize 


MIN_DF = 10
TRAIN = True


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
    # Create temporary views in order to create the joined data
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


def main(context):
    """Main function takes a Spark SQL context."""
	
    # TASK 1
    try:
        comments = context.read.parquet("comments.parquet")
        submissions = context.read.parquet("submissions.parquet")
        labeled_data = context.read.parquet("labeled_data.parquet")
    except FileNotFoundError:
        comments = context.read.json("comments-minimal.json.bz2")
        comments.write.parquet("comments.parquet")

        submissions = context.read.json("submissions.json.bz2")
        submissions.write.parquet("submissions.parquet")

        labeled_data.write.parquet("labeled_data.parquet") 
        labeled_data = context.read.csv("labeled_data.csv", header='true')

    # TASK 4
    context.registerFunction("sanitize", modified_sanitize, ArrayType(StringType()))

    # TASK 5
    joined_data = generate_joined_data(labeled_data, comments, context)
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

    # TASK 6A
    # REFERENCE:
    # https://spark.apache.org/docs/latest/ml-features.html#countvectorizer

    vectorizer = CountVectorizer(inputCol="body",
                                 outputCol="features",
                                 minDF=MIN_DF,
                                 binary=True)
    model = vectorizer.fit(ngrams)

    # TASK 6B
    result = model.transform(ngrams)
    result.createOrReplaceTempView("result")

    djt_sentiment_sql = """
SELECT
    *,
    if (labeldjt = 1, 1, 0) AS pos_label,
    if (labeldjt = -1, 1, 0) AS neg_label
FROM result"""
    sentiment_data = context.sql(djt_sentiment_sql)
    sentiment_data.show()


    # TASK 7
    # Initialize two logistic regression models.
    # Replace labelCol with the column containing the label, and featuresCol with the column containing the features.
    if TRAIN:
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
        posModel = posCrossval.fit(posTrain)
        print("Training negative classifier...")
        negModel = negCrossval.fit(negTrain)

        # Once we train the models, we don't want to do it again. We can save the models and load them again later.
        posModel.save("project2/pos.model")
        negModel.save("project2/neg.model")

    # TASK 8
    joined_data = generate_joined_data(labeled_data, comments, context)


if __name__ == "__main__":
    conf = SparkConf().setAppName("CS143 Project 2B")
    conf = conf.setMaster("local[*]")
    sc   = SparkContext(conf=conf)
    sqlContext = SQLContext(sc)
    sc.addPyFile("cleantext.py")
    main(sqlContext)
