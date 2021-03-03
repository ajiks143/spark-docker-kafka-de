from pyspark.sql.functions import *

from dependencies.spark import start_spark

def main():
    """
    Main code for processing Real time Data pipeline
    :return: None
    """

    spark, logs = start_spark(app_name='realtime_job')

    logs.info('Real time job is up-and-running')

    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:19092") \
        .option("subscribe", "xapo1") \
        .option("startingOffsets", "earliest") \
        .load()

    string_df = df.selectExpr("CAST(value AS STRING)")

    json_schema = spark.read.json(string_df.rdd.map(lambda row: row.json)).schema

    json_df = string_df.withColumn("jsonData", from_json(col("value"), json_schema)).select("jsondata.*")

    json_df.printSchema()

    
    #string_df \
    #    .writeStream \
    #    .outputMode("append") \
    #    .format("console") \
    #    .option("truncate", "false") \
    #    .start().awaitTermination()

    logs.info('Real time job is finished')
    spark.stop()
    return None

# entry point for PySpark application
if __name__ == '__main__':
    main()