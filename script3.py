import sys
import os
import json
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.0 pyspark-shell'
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split

spark = SparkSession \
    .builder \
    .appName("kafka_to_parquet") \
    .getOrCreate()
# Subscribe to 1 topic
df_queue2 = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "localhost:9092") \
  .option("subscribe", "queue2") \
  .option("startingOffsets", "earliest") \
  .load()

df_queue3 = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "localhost:9092") \
  .option("subscribe", "queue3") \
  .option("startingOffsets", "earliest") \
  .load()

query_queue2 = df_queue2 \
    .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
    .writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("checkpointLocation", "/Users/ludo/Desktop/UntieNots/queue2") \
    .option("path", "/Users/ludo/Desktop/UntieNots/queue2") \
    .start()

query_queue3 = df_queue3 \
    .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
    .writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("checkpointLocation", "/Users/ludo/Desktop/UntieNots/queue3") \
    .option("path", "/Users/ludo/Desktop/UntieNots/queue3") \
    .start()

query_queue2.awaitTermination()
query_queue3.awaitTermination()