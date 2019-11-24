import sys
import os
import json
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.4.4 pyspark-shell'
from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from uuid import uuid1
from kafka import KafkaProducer


def queue2_filter(x):
	data = json.loads(x[1])
	for topic in topic_list:
		if data["word"] in topic_list[topic]:
			return True
	return False

def queue3_filter(x):
	data = json.loads(x[1])
	return data["word"] in topic_list

def send_to_queue2(iter):
	producer = KafkaProducer(bootstrap_servers='localhost:9092')

	for record in iter:
		data = json.loads(record[1])
		topics=[]
		for topic in topic_list:
			if data["word"] in topic_list[topic]:
				topics.append(topic)
		data["topics"]=topics
		producer.send("queue2", key=bytes(record[0], encoding='utf-8'), value=json.dumps(data).encode('utf-8'))
	producer.flush()
	producer.close()

def send_to_queue3(iter):
	producer = KafkaProducer(bootstrap_servers='localhost:9092')
	for record in iter:
		data = json.loads(record[1])
		producer.send("queue3", key=bytes(record[0], encoding='utf-8'), value=json.dumps(data).encode('utf-8'))
	producer.flush()
	producer.close()

topic_list = {"sport": ['doping', 'olympic', 'injury','medal','record','title','world','athletic'], \
			"tech" : ['technology','author','article','computer','hi-tech','software','network','security','phone']}

sc = SparkContext(appName='PythonStreamingRecieverKafka')
ssc = StreamingContext(sc, 2) # 2 second window
zookeeper_broker = "localhost:2181"
topic = "queue1"
kvs = KafkaUtils.createStream(ssc, \
							zookeeper_broker, \
							'streaming-consumer',\
							{topic:1})
#kvs.pprint(num=10)
kvs.pprint(num=10)
queue2 = kvs.filter(lambda x: queue2_filter(x))
queue2.pprint(num=10)
queue2.foreachRDD(lambda rdd: rdd.foreachPartition(lambda x:send_to_queue2(x)))
queue3 = kvs.filter(lambda x: queue3_filter(x))
queue3.pprint(num=10)
queue3.foreachRDD(lambda rdd: rdd.foreachPartition(lambda x:send_to_queue3(x)))


ssc.start()
ssc.awaitTermination()

