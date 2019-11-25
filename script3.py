########################################
#
# SCRIPT 3
#
########################################
#* Read queue Q2 since last offset and save its content in .parquet files 
#* Read queue Q3 since last offset and save its content in .parquet files
########################################

#Gestion des packages et dépendances
import sys
import os
import json
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.0 pyspark-shell'
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split

# Parquet directory
queue2_parquet_directory = "./queue2"
queue3_parquet_directory = "./queue3"

# Création de notre sparkContexte
spark = SparkSession \
    .builder \
    .appName("kafka_to_parquet") \
    .getOrCreate()

# Utilsiation de : Structured Streaming + Kafka Integration Guide (Kafka broker version 0.10.0 or higher)

# Lecture de notre queue2
df_queue2 = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "localhost:9092") \
  .option("subscribe", "queue2") \
  .option("startingOffsets", "earliest") \
  .load()

# Lecture de notre queue3
df_queue3 = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "localhost:9092") \
  .option("subscribe", "queue3") \
  .option("startingOffsets", "earliest") \
  .load()

# Création de notre dataframe a partir de la queue2 et stockage sous parquet
query_queue2 = df_queue2 \
    .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
    .writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("checkpointLocation", queue2_parquet_directory) \
    .option("path", queue2_parquet_directory) \
    .start()

# Création de notre dataframe a partir de la queue3 et stockage sous parquet
query_queue3 = df_queue3 \
    .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
    .writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("checkpointLocation", queue3_parquet_directory) \
    .option("path", queue3_parquet_directory) \
    .start()

query_queue2.awaitTermination()
query_queue3.awaitTermination()