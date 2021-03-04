#! /bin/bash

echo "***Running the Spark batch job***"
$SPARK_HOME/bin/spark-submit /app/xapo/job/Batch.py
echo "***Spark batch job completed***"

echo "***Running the Kafka Producer job***"
python3 /app/xapo/job/Producer.py
echo "***Kafka Producer job completed***"

$SPARK_HOME/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.2,org.apache.kafka:kafka-clients:2.7.0 /app/xapo/job/Real-Time.py