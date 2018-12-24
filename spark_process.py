from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row, SQLContext
import sys
import requests

conf = SparkConf().setAppName('TwitterStream')

sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")

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

