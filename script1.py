########################################
#
#	SCRIPT 1
#
########################################
# Open a directory containing text files from a corpus of your choice.
# Split text files in words.
# Send them in a kafka queue (Q1) : {"source": <file_name >, "word": <word>}
########################################


import json
from pathlib import Path
directory_sport = "./bbc-dataset/sport"
directory_tech = "./bbc-dataset/tech"

from kafka import KafkaProducer
kafka_host = 'localhost:9092'
producer = KafkaProducer(bootstrap_servers=kafka_host)
topic="queue1"

def split_text_to_words(text):
	from nltk.tokenize import word_tokenize
	tokens = word_tokenize(text)
	# convert to lower case
	tokens = [w.lower() for w in tokens]
	
	# remove punctuation from each word
	import string
	table = str.maketrans('', '', string.punctuation)
	
	stripped = [w.translate(table) for w in tokens]
	# remove remaining tokens that are not alphabetic
	words = [word for word in stripped if word.isalpha()]
	# filter out stop words
	from nltk.corpus import stopwords
	stop_words = set(stopwords.words('english'))
	words = [w for w in words if not w in stop_words]
	#print(words[:100]) 
	return words

for file in Path(directory_sport).iterdir():
	print(file)
	file = open(file, 'rt',encoding="ISO-8859-1")
	text = file.read()
	file.close()
	
	for word in split_text_to_words(text):
		data ={}
		data["source"]=file.name
		data["word"]=word
		# send to topic on broker
		producer.send(topic, key=bytes("sport", encoding='utf-8'), value=json.dumps(data).encode('utf-8'))


for file in Path(directory_tech).iterdir():
	print(file)
	file = open(file, 'rt',encoding="ISO-8859-1")
	text = file.read()
	file.close()

	for word in split_text_to_words(text):
		data ={}
		data["source"]=file.name
		data["word"]=word
		# send to topic on broker
		producer.send(topic, key=bytes("tech", encoding='utf-8'), value=json.dumps(data).encode('utf-8'))

