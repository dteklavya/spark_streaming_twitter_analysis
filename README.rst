==================================
Twitter Analysis with Apache Spark
==================================

This code repository provides ways to get live twitter data and use Spark Streaming with MLlib for Analysis.

Twitter setup
-------------

1. Got to https://developer.twitter.com/ and create a new app if you don't already have one.

2. Generate the keys and tokens and save them in config.py. CAUTION: Never add config.py to git.

3. Setup the callback URL in twitter app settings and save the same in config.py.


Apache Spark Setup
------------------

This code works with Spark version 2.4, download here_.

.. _here https://spark.apache.org/releases/spark-release-2-4-0.html

Once downloaded, make sure that environment variables SPARK_HOME and PYTHONPATH point to /path/to/spark//spark-2.4.0 and /path/to/spark//spark-2.4.0/python respectively.

Also add /path/to/spark//spark-2.4.0/bin to your PATH environment variable.


Python Setup
------------

Create a virtual environment with Python version 3.4 and install dependencies from requirements.txt.


Quick Start
-----------

1. Start Twitter streaming like so::

   $ python twitter_stream.py

2. Start Spark process that reads tweets from above stream for processing::

    $ python spark_process.py
    
3. Start the Flask app like so::

		python webapp/app.py
    
4. Once all the services are running, point the browser to http://localhost:9999/

5. The page displays counts of positive and negative tweets received.


Walkthrough
-----------

The spark process from step two above reads tweets from twitter stream at step one. It then computes the tweet sentiment
using Naive Bayes model and save results on Redis.

The Flask web application displays results using Angular JS displaying a live data i.e. the counts of positive and negative tweets.  


TODO
----

* Find a way to save the Naive Bayes model after training. As of now spark doesn't allow models with custom transformers to be saved to disk.

* Use Kafka instead of a TCP socket to send tweet streams to spark.

* Find a way to send results directly to redis from Spark process.

* Verify model's accuracy using Stanford Core NLP.

* Web template needs lot of improvement. 



