from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row, SQLContext
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, StringType, IntegerType

from config import SENTIMENT140_DATA
from pyspark.ml.classification import NaiveBayes
from pyspark.ml.linalg import Vectors, Vector
from pyspark.ml.feature import HashingTF, Tokenizer
from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.feature import StopWordsRemover

import nltk
from pyspark import keyword_only
from pyspark.ml import Transformer
from pyspark.ml.param.shared import HasInputCol, HasOutputCol, Param
from pyspark.sql.functions import udf
from pyspark.sql.types import ArrayType
from pyspark.sql.functions import lit

import re


class TweetSanitizer(Transformer, HasInputCol, HasOutputCol):

    @keyword_only
    def __init__(self, inputCol=None, outputCol=None, stopwords=None):
        super(TweetSanitizer, self).__init__()
        self.stopwords = Param(self, "stopwords", "")
        self._setDefault(stopwords=set())
        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    def setParams(self, inputCol=None, outputCol=None, stopwords=None):
        kwargs = self._input_kwargs
        return self._set(**kwargs)

    def setStopwords(self, value):
        self._paramMap[self.stopwords] = value
        return self

    def getStopwords(self):
        return self.getOrDefault(self.stopwords)

    def _transform(self, dataset):
        stopwords = self.getStopwords()

        def f(s):
            s = re.sub("(?:http|https?)://[\w/%.-]+", "", s)
            s = s.replace("\n", "")

            s = re.sub("rt\s+", "", s)
            s = re.sub("\s+@\w+", "", s)
            s = re.sub("@\w+", "", s)
            s = re.sub("\s+\#\w+", "", s)
            s = re.sub("\#\w+", "", s)
            tokens = nltk.tokenize.wordpunct_tokenize(s)
            results = [t for t in tokens if t.lower() not in stopwords]
            return results

        t = ArrayType(StringType())
        out_col = self.getOutputCol()
        in_col = dataset[self.getInputCol()]
        return dataset.withColumn(out_col, udf(f, t)(in_col))


def process_row(row):
#     print(row.toJSON().collect())
    prediction, count = row
    sentiment = ''
    if prediction == 1:
        sentiment = 'Positive'
    else:
        sentiment = 'Negative'
    print(sentiment, count)
    import requests

    url = 'http://localhost:9999/updateData'
    request_data = {'sentiment': sentiment, 'count': str(count)}
    response = requests.post(url, data=request_data)
    print(response)

#     status, label, filtered, features, rawPrediction, probability, prediction, pred_agg = row
#     print(label, prediction, pred_agg)


def read_twitter_stream(model, sc):

    spark = SparkSession(sc)

    socketDF = spark \
    .readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", 9009) \
    .load()

    # Add a field 'label' and...
    # Rename the column to 'status'

    socketDF = socketDF.withColumn("label", lit(0))
    socketDF = socketDF.withColumnRenamed("value", "status")
#     print("COLUMNS:", socketDF.columns, socketDF["status"])
#     socketDF.printSchema()

    prediction = getTweetSentiment(model, socketDF)

#     print(prediction)
    from pyspark.sql.functions import sum as _sum
    prediction.printSchema()
    print(prediction.schema)
    prediction = prediction.groupBy("prediction") \
                    .count().alias('sentiment_sum')

    prediction.printSchema()
    print(prediction.schema)
#     print(prediction.toJSON().collect())
    query1 = prediction.writeStream \
            .outputMode("update") \
            .foreach(process_row) \
            .start()

#     selected = prediction.select("label", "status", "probability", "prediction", "filtered")
#     for row in selected.collect():
#         rid, text, prob, prediction, filtered = row
#         print("(%d, %s) --> prob=%s, prediction=%f, filtered=%s" % (rid, text, str(prob), prediction, filtered))

    # wait for the streaming to finish
    query1.awaitTermination()
#     socketDF.show(n=1, truncate=False)


def loadSentiment140(sc, path):
    # Create spark session for Dataset and DataFrame API
    spark = SparkSession(sc)

    '''
        Another way to create SparkSession -

        spark = SparkSession \
            .builder \
            .appName("Python Spark SQL basic example") \
            .config("spark.some.config.option", "some-value") \
            .getOrCreate()

    '''

    # Defined Dataframe columns for Sentiment140 data
    cols = ["label", "id", "date", "query", "user", "status"]

    schema = [ StructField('label', IntegerType(), True),
               StructField('id', StringType(), True),
               StructField('date', StringType(), True),
               StructField('query', StringType(), True),
               StructField('user', StringType(), True),
               StructField('status', StringType(), True),
                ]
    schema = StructType(schema)

    # Load data from CSV and get DataFrame.
    df = spark.read.csv(path, schema=schema)

    # Drop unwanted columns
    df = df.drop("id", "date", "query", "user")
    return (df, spark)


def getOrCreateNBModel(sc):

    # Load the pipeline from disk.
    loaded = PipelineModel.load('./nbmodel')

    # Returned the model loaded from the disk if found
#     if loaded:
#         return loaded

    # Else create the model/PipelineModel, save and return it.

    (df, spark) = loadSentiment140(sc, SENTIMENT140_DATA)

#     tokenizer = Tokenizer(inputCol='status', outputCol='barewords')

#     remover = StopWordsRemover(inputCol='barewords', outputCol='filtered')# , stopWords=removeStopWords())
#     print('Remover', remover.transform(df).head())

    tokenizer = TweetSanitizer(inputCol='status', outputCol='filtered')

    hashingTF = HashingTF(inputCol=tokenizer.getOutputCol(), outputCol='features')

    # Defined model parameters
    nb = NaiveBayes(smoothing=1.0, modelType="multinomial")

    # Defined Pipeline
    pipeline = Pipeline(stages=[tokenizer, hashingTF, nb])

    # Train the data
    model = pipeline.fit(df)

    # Save the pipeline, overwrite if already present.
    # This won't work with PySpark's custom transformer
#     model.write().overwrite().save('./nbmodel')

    return model


def transformFeatures(tweetText):
    HashingTF(tweetText)


def removeStopWords():
    with open('NLTK_English_Stopwords_Corpus.txt', 'r') as f:
        lines = f.read().splitlines()
    return lines


def getTweetSentiment(model, df):
    prediction = model.transform(df)
    return prediction


# Initialize the spark config
conf = SparkConf().setAppName('TwitterStream').setMaster("local[*]")

# Create the spark context
sc = SparkContext.getOrCreate(conf=conf)

# Suppress debug messages
sc.setLogLevel("ERROR")

# Create Naive Bayes model
model = getOrCreateNBModel(sc)

read_twitter_stream(model, sc)

sc.stop()
