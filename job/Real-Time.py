from pyspark.sql.functions import *
from pyspark.sql.types import *
from dependencies.spark import start_spark

def main():
    """
    Main code for processing Real time Data pipeline
    :return: None
    """

    spark, logs = start_spark(app_name='realtime_job')

    logs.info('Real time job is up-and-running')

    schema = spark.read.options(multiLine=True).json("data/input/test.json").schema

    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:19092") \
        .option("subscribe", "xapo2") \
        .option("startingOffsets", "earliest") \
        .load()

    string_df = df.selectExpr("CAST(value AS STRING)")

    json_df = string_df \
        .withColumn("jsonData", from_json(col("value"), schema)) \
        .select("jsonData.block_height","jsonData.block_time", "jsonData.fee", "jsonData.outputs") \
        .withColumn("exp_outputs",explode(col("outputs"))) \
        .select("block_height","block_time","fee","exp_outputs.*") \
        .select("block_height","block_time","fee","addresses","value") \
        .withColumn("exp_address", explode(col("addresses"))) \
        .select("block_height","block_time","fee",col("exp_address").alias("addresses"),"value") \
        .selectExpr("block_height","to_timestamp(block_time) as block_time","fee","addresses","value") \
        .filter(col("addresses") != "")
    json_df.printSchema()
    
    json_df \
        .writeStream \
        .format("csv") \
        .option("path","data/output/streamoutput/") \
        .trigger(processingTime='10 seconds') \
        .option("checkpointLocation", "/tmp/checkpoint/") \
        .option("header", True) \
        .outputMode("append") \
        .start() \
        .awaitTermination()

    logs.info('Real time job is finished')
    #spark.stop()
    return None

# entry point for PySpark application
if __name__ == '__main__':
    main()