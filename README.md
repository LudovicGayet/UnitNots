## UnitNots

Ce projet contient :
- 4 scripts codés en python
- Un jeu de données provenant de la bbc contenant des articles traitant de sport ou de technology (nos deux topics)
- Des captures d'écrans des résultats des script 1 et 2
- Un exemple des fichiers .parquet généré par le script 3 + fichier de gestion des offsets
- Des captures d'écrans des résultats du script 4


L'environnement de développement fut le suivant :
```
- spark 2.4.4 => utilisation de l'api python
- java 1.8
```

L'environnement python :
```
- python 3.7.5
- packages installés décrit dans le fichier environment.yml
```

### Script 1 : 
Etape préliminaire de nettoyage du texte (python ntlk).
Envoie de données dans une queue Kafka(Q1) : {"source": <file_name >, "word": <word>} 
Dans le "topic" kafka, on garde pour clé notre "topic" (sport ou tech) en clé et en valeur on donne un json contenant les informations voulues. Garder le topic nous permet dans la partie analyse de connaître le veritable topic auquel appartient notre fichier.
![Résultat du scritp1](https://github.com/LudovicGayet/UntieNots/blob/master/screenshot%20queue1.png)
  

### Script 2 : 
Pour cette partie, nous avons utilisés les topics/mots clés suivants:
```
topic_list = {"sport": ['doping', 'olympic', 'injury','medal','record','title','world','athletic'], \
		"tech" : ['technology','author','article','computer','hi-tech','software','network','security','phone']}
```
Les données sont ingérées en Spark Streaming vers la queue2: {"source": <file_name >, "word": <word>, "topics": [<topics>] } <br>
![Résultat du scritp2](https://github.com/LudovicGayet/UntieNots/blob/master/screenshot%20queue2.png)
<br>
ou vers la queue3: {"source": <file_name>, "topic": <topic>}
<br>
![Résultat du scritp3](https://github.com/LudovicGayet/UntieNots/blob/master/screenshot%20queue3.png)
<br>

### Script 3 : 
Nous persistons les données des queue2 et queue3 au format parquet avec gestion des offsets. <br>
Toutes ces données sont disponibles dans les dossiers queue2 et queue3 qui contiennent les fichiers parquets mais aussi les fichiers relatifs aux offsets. <br>

### Script 4 : 
Ce script s'interesse à l'analyse des données stockées préalablement au format parquet.
Plusieurs indicateurs sont à fournir:
```
* For each topic find :
- the sources associated with the number of occurrences for each key word.
- the false positives (sources identified with the keywords that do not belong to the topic) => We assume that a source belongs to a topic if X% of its keywords can be found in the source. (X is an argument of the script).
- the relevance of each keyword: rate of presence in a source belonging to the topic/ rate of presence in a topic not belonging to the topic / rate of absence in a topic that belonged to the topic
```
