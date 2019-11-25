## UnitNots

Ce projet contient :
- 4 scripts codés en python
- Un jeu de données provenant de la bbc contenant des articles traitant de sport ou de technology (nos deux topics)
- Des captures d'écrans des résultats des script 1 et 2
- Un exemple des fichiers .parquet généré par le script 3


L'environnement de développement fut le suivant :
- spark 2.4.4
- java 1.8

L'environnement python :
- décrit dans le fichier environment.yml

Script 1 : Envoie de données dans une queue Kafka(Q1) : {"source": <file_name >, "word": <word>} 
  Dans le "topic" kafka, on garde pour clé notre "topic" (sport ou tech) en clé et en valeur on donne un json contenant les informations voulues
![Résultat du scritp1](https://github.com/LudovicGayet/UntieNots/blob/master/screenshot%20queue1.png)
  
![Résultat du scritp2](https://github.com/LudovicGayet/UntieNots/blob/master/screenshot%20queue2.png)
![Résultat du scritp3](https://github.com/LudovicGayet/UntieNots/blob/master/screenshot%20queue3.png)

