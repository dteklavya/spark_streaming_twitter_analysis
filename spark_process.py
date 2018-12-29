from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row, SQLContext
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, StringType, IntegerType

import sys
import requests

from config import SENTIMENT140_DATA
from pyspark.ml.classification import NaiveBayes
from pyspark.ml.linalg import Vectors, Vector
from pyspark.ml.feature import HashingTF, Tokenizer
from pyspark.ml import Pipeline, PipelineModel


def read_twitter_stream(sc):
    ssc = StreamingContext(sc, 2)

    ssc.checkpoint("checkpoint_TwitterApp")

    dataStream = ssc.socketTextStream("localhost", 9009)

    words = dataStream.flatMap(lambda line: print(line))

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
    if loaded:
        return loaded

    # Else create the model/PipelineModel, save and return it.

    (df, spark) = loadSentiment140(sc, SENTIMENT140_DATA)

    tokenizer = Tokenizer(inputCol='status', outputCol='barewords')
    hashingTF = HashingTF(inputCol=tokenizer.getOutputCol(), outputCol='features')

#     df.foreach(transformFeatures(df.status))

    # Defined model parameters
    nb = NaiveBayes(smoothing=1.0, modelType="multinomial")

    # Defined Pipeline
    pipeline = Pipeline(stages=[tokenizer, hashingTF, nb])

    # Train the data
    model = pipeline.fit(df)

    # Save the pipeline, overwrite if already present.
    model.write().overwrite().save('./nbmodel')

    return model


def transformFeatures(tweetText):
    HashingTF(tweetText)


# Initialize the spark config
conf = SparkConf().setAppName('TwitterStream').setMaster("local[*]")

# Create the spark context
sc = SparkContext.getOrCreate(conf=conf)
# Suppress debug messages
sc.setLogLevel("ERROR")

# loadSentiment140(sc, SENTIMENT140_DATA)
model = getOrCreateNBModel(sc)
spark = SparkSession(sc)

# Get prediction from loaded PipelineModel
test = spark.createDataFrame([(
    1, 'Just tweeting!'
    )], ['label', 'status'])
test.head()
prediction = model.transform(test)

selected = prediction.select("label", "status", "probability", "prediction")
for row in selected.collect():
    rid, text, prob, prediction = row
    print("(%d, %s) --> prob=%s, prediction=%f" % (rid, text, str(prob), prediction))

sc.stop()
