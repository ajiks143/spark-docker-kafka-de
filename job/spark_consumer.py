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

    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:19092") \
        .option("subscribe", "xapo1") \
        .option("startingOffsets", "earliest") \
        .load()

    string_df = df.selectExpr("CAST(value AS STRING)")

    json_schema = StructType(List(StructField(block_hash,StringType,true),StructField(block_height,LongType,true),StructField(block_time,LongType,true),StructField(confirmations,LongType,true),StructField(fee,LongType,true),StructField(hash,StringType,true),StructField(inputs,ArrayType(StructType(List(StructField(prev_addresses,ArrayType(StringType,true),true),StructField(prev_position,LongType,true),StructField(prev_tx_hash,StringType,true),StructField(prev_type,StringType,true),StructField(prev_value,LongType,true),StructField(sequence,LongType,true))),true),true),StructField(inputs_count,LongType,true),StructField(inputs_value,LongType,true),StructField(is_coinbase,BooleanType,true),StructField(is_double_spend,BooleanType,true),StructField(is_sw_tx,BooleanType,true),StructField(lock_time,LongType,true),StructField(outputs,ArrayType(StructType(List(StructField(addresses,ArrayType(StringType,true),true),StructField(spent_by_tx,StringType,true),StructField(spent_by_tx_position,LongType,true),StructField(type,StringType,true),StructField(value,LongType,true))),true),true),StructField(outputs_count,LongType,true),StructField(outputs_value,LongType,true),StructField(sigops,LongType,true),StructField(size,LongType,true),StructField(version,LongType,true),StructField(vsize,LongType,true),StructField(weight,LongType,true),StructField(witness_hash,StringType,true)))

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