from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row, SQLContext
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, StringType

import sys
import requests

from config import SENTIMENT140_DATA

# Initialize the spark config
conf = SparkConf().setAppName('TwitterStream').setMaster("local[*]")

# Create the spark context
sc = SparkContext.getOrCreate(conf=conf)
# Suppress debug messages
sc.setLogLevel("ERROR")


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
    cols = ["polarity", "id", "date", "query", "user", "status"]
    # Defined schema
    schema = [ StructField(fname, StringType(), True) for fname in cols ]
    schema = StructType(schema)

    # Load data from CSV and get DataFrame.
    df = spark.read.csv(path, schema=schema)

    # Drop unwanted columns
    df = df.drop("id", "date", "query", "user")
    df.show()


loadSentiment140(sc, SENTIMENT140_DATA)
sc.stop()

