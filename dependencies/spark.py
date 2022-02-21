"""
spark.py
~~~~~~~~

Module containing helper function for use with Apache Spark
"""

import __main__

from os import environ, listdir, path
import json
from pyspark import SparkFiles
from pyspark.sql import SparkSession

from dependencies import logging

def startSpark(appName='sparkapp'):
    spark = SparkSession.builder.appName(appName).getOrCreate()
    spark.sparkContext.setLogLevel('ERROR')
    spark_logger = logging.Log4j(spark)
    return spark, spark_logger