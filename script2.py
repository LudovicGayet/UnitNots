########################################
#
#	SCRIPT 2
#
########################################
#* Load a list of topics that should be monitored (a topic has a name and a list of keywords, ex {"color": ['red', 'blue', 'green']}, {"sport": ['football', 'tennis', 'horseriding']} {"plane": ['wing', 'pilot', 'propeller']})
#* Read queue Q1 with spark streaming :
#- If the word is in the keyword list for one or several topics, send a message in another queue Q2: {"source": <file_name >, "word": <word>, "topics": [<topics>] }
#- If the word corresponds to a topic name, send in a third queue Q3: {"source": <file_name>, "topic": <topic>}
########################################

# Gestion des packages et dépendances
import sys
import os
import json
# Attention problème de compabilité si mauvais package utilisé en fonction de la version de spark
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.4.4 pyspark-shell'
from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from kafka import KafkaProducer


def queue2_filter(x):
# User define fonction pour filtrer notre DStream provenant de la queue1 et garder les messages à destinations de notre queue2
	data = json.loads(x[1])
	for topic in topic_list:
		if data["word"] in topic_list[topic]:
			return True
	return False

def queue3_filter(x):
# User define fonction pour filtrer notre DStream provenant de la queue1 et garder les messages à destinations de notre queue3
	data = json.loads(x[1])
	return data["word"] in topic_list

def send_to_queue2(rdd):
# gere l'envoie des messages à destinations de la queue2

# le producer kafka doit obligatoirement s'initialiser ici
# l'instancier avant entrainerait des erreurs de sérialisation
	producer = KafkaProducer(bootstrap_servers='localhost:9092')

	for record in rdd:
		data = json.loads(record[1])
		topics=[]
		for topic in topic_list:
			if data["word"] in topic_list[topic]:
				topics.append(topic)
		data["topics"]=topics
		producer.send("queue2", key=bytes(record[0], encoding='utf-8'), value=json.dumps(data).encode('utf-8'))
	producer.flush()
	producer.close()

def send_to_queue3(rdd):
# gere l'envoie des messages à destinations de la queue3

# le producer kafka doit obligatoirement s'initialiser ici
# l'instancier avant entrainerait des erreurs de sérialisation
	producer = KafkaProducer(bootstrap_servers='localhost:9092')
	for record in rdd:
		data = json.loads(record[1])
		producer.send("queue3", key=bytes(record[0], encoding='utf-8'), value=json.dumps(data).encode('utf-8'))
	producer.flush()
	producer.close()



# Initialisation de notre liste de topics et mots-clés associés
topic_list = {"sport": ['doping', 'olympic', 'injury','medal','record','title','world','athletic'], \
			"tech" : ['technology','author','article','computer','hi-tech','software','network','security','phone']}

# Création de notre Stream.
sc = SparkContext(appName='PythonStreamingRecieverKafka')
ssc = StreamingContext(sc, 2) # 2 second window
zookeeper_broker = "localhost:2181"
topic = "queue1"
kvs = KafkaUtils.createStream(ssc, \
							zookeeper_broker, \
							'streaming-consumer',\
							{topic:1})

# séparation de notre stream et filtrage avant envoie dans queue2 et queue3
queue2 = kvs.filter(lambda x: queue2_filter(x))
#queue2.pprint(num=10)
queue2.foreachRDD(lambda rdd: rdd.foreachPartition(lambda x:send_to_queue2(x)))
queue3 = kvs.filter(lambda x: queue3_filter(x))
#queue3.pprint(num=10)
queue3.foreachRDD(lambda rdd: rdd.foreachPartition(lambda x:send_to_queue3(x)))


ssc.start()
ssc.awaitTermination()

