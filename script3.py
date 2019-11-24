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
df = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "localhost:9092") \
  .option("subscribe", "queue2,queue3,") \
  .option("startingOffsets", "earliest") \
  .load()

df.printSchema()

query = df \
    .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
    .writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("checkpointLocation", "/Users/ludo/Desktop/UntieNots/") \
    .option("path", "/Users/ludo/Desktop/UntieNots/") \
    .start()

query.awaitTermination()