########################################
#
#	SCRIPT 1
#
########################################
# Open a directory containing text files from a corpus of your choice.
# Split text files in words.
# Send them in a kafka queue (Q1) : {"source": <file_name >, "word": <word>}
########################################



from pathlib import Path
directory = "./bbc-dataset/sport"

from kafka import KafkaProducer
producer = KafkaProducer(bootstrap_servers='localhost:9092')

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

for file in Path(directory).iterdir():
	print(file)
	file = open(file, 'rt',encoding="ISO-8859-1")
	text = file.read()
	file.close()

	for word in split_text_to_words(text):
		# message value and key must be raw bytes
		word_bytes = bytes(word, encoding='utf-8')
		file_bytes = bytes(file.name, encoding='utf-8')
		# send to topic on broker
		producer.send('test', key=file_bytes, value=word_bytes)

