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
from pyspark import keyword_only  # # < 2.0 -> pyspark.ml.util.keyword_only
from pyspark.ml import Transformer
from pyspark.ml.param.shared import HasInputCol, HasOutputCol, Param
from pyspark.sql.functions import udf
from pyspark.sql.types import ArrayType, StringType

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


def read_twitter_stream(sc):
    ssc = StreamingContext(sc, 2)

    ssc.checkpoint("checkpoint_TwitterApp")

    dataStream = ssc.socketTextStream("localhost", 9009)

    words = dataStream.flatMap(lambda line: print("IS THIS LINE:", line))
    print(words, type(dataStream))

    dataStream.pprint(num=2)

    # start the streaming computation
    ssc.start()
    # wait for the streaming to finish
    try:
        ssc.awaitTermination()
    except KeyboardInterrupt:
        print("STOPPING...!!!")
        ssc.stop()


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


# Initialize the spark config
conf = SparkConf().setAppName('TwitterStream').setMaster("local[*]")

# Create the spark context
sc = SparkContext.getOrCreate(conf=conf)

# Suppress debug messages
sc.setLogLevel("ERROR")

# read_twitter_stream(sc)
# exit(0)

# loadSentiment140(sc, SENTIMENT140_DATA)
model = getOrCreateNBModel(sc)
spark = SparkSession(sc)

# Get prediction from loaded PipelineModel
test = spark.createDataFrame([(
    1, 'Just tweeting!'
    )], ['label', 'status'])
test.head()
prediction = model.transform(test)
print(prediction)

selected = prediction.select("label", "status", "probability", "prediction", "filtered")
for row in selected.collect():
    rid, text, prob, prediction, filtered = row
    print("(%d, %s) --> prob=%s, prediction=%f, filtered=%s" % (rid, text, str(prob), prediction, filtered))

sc.stop()
