from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType  
import json

spark = SparkSession.builder.master('local[*]').appName('sample').getOrCreate()

df = spark.read.format("json").options(multiLine=True).load("data/input/test.json")
schema_json = df.schema.json()
print(schema_json)

# Restore schema from json:

new_schema = StructType.fromJson(json.loads(schema_json))
print(new_schema)