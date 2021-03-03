from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.master('local[*]').appName('sample').getOrCreate()

df = spark.read.json("data/input/test.json").cache()
json_schema = spark.read.json(df.rdd.map(lambda row: row.json)).schema
#df.withColumn('json', from_json(col('json'), json_schema))
json_schema.printSchema()