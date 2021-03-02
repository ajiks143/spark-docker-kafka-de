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


def start_spark(app_name='my_spark_app', master='local[*]',
                files=[], spark_config={}):
    """
    Set the spark session
    :param app_name: Name of Spark app.
    :param master: Cluster connection details (defaults to local[*]).
    :param files: List of files to send to Spark cluster (master and
        workers).
    :param spark_config: Dictionary of config key-value pairs.
    :return: A tuple of references to the Spark session and logger.
    """
    spark_builder = (
        SparkSession
            .builder
            .master(master)
            .appName(app_name))

    spark_files = ','.join(list(files))
    spark_builder.config('spark.files', spark_files)

    # add other config params
    for key, val in spark_config.items():
        spark_builder.config(key, val)

    # create session and retrieve Spark logger object
    spark = spark_builder.getOrCreate()
    spark_logger = logging.Log4j(spark)

    return spark, spark_logger

