

# Exercices
- Importer dans Eclipse en tant que projet Eclipse le modèle d'application Spark Batch `tp-spark-batch` mis à disposition à côté de ce fichier README.md
- Ouvrir une invite de commande DOS à la racine, et exécuter la commande de compilation. Cette commande est nécessaire car le plugin Eclipse "embedded" n'est pas capable de résoudre les dépendances via un proxy NTLM tel que présent à CAAS.
```
mvn package assembly:single
```
- Une fois les dépendances résolues, sous Eclipse cliquer sur la racine du projet `tp-spark-batch` *> Maven > Update Project...* pour résoudre les erreurs de dépendance.

> Documentations de référence à utiliser comme aide pour les
> exercices :
> - [Guide de développement RDD](https://spark.apache.org/docs/latest/rdd-programming-guide.html)
> - [Guide de développement DataFrame, Dataset et SparkSQL](https://spark.apache.org/docs/latest/sql-programming-guide.html)
> - [Javadoc JavaRDD](https://spark.apache.org/docs/latest/api/java/org/apache/spark/api/java/JavaRDD.html)
> - [Javadoc Dataset](https://spark.apache.org/docs/latest/api/java/org/apache/spark/sql/Dataset.html)

> Pour tous les exercices, mettre des commentaires dans le code pour rappeler la nature des opérations demandées à être implémentées

## Manipulation de RDD

### Exécution d'une application dans l'IDE ###

- Ouvrir le fichier `pom.xml`, visualiser un POM Maven minimal d'une application Spark
- Ouvrir le fichier ` formation.sparkbatch.BatchApp.java`, visualiser le contenu minimale d'une application Spark 

- Exécuter sous Eclipse l'application comme une application Java. L'exécution échoue.
- Ajouter la configuration `VM argument` suivante pour permettre une exécution de l'application en mode local (cluster Spark simulé dans une JVM). 
    ```
    -Dspark.master=local[1]
    ```
   - Vérifier la bonne exécution de l'application
- Ajouter un point d'arrêt Java (*breakpoint*) au niveau de l'instruction `collect()` et lancer l'application en mode debug.
	- Lorsque le point d'arrêt est touché, chercher dans les logs console l'URL de la *SparkUI* et l'ouvrir dans un navigateur
	- Visualiser le contenu de la console Java, ainsi que le contenu des écrans *Jobs* et *Stages* de la console web SparkUI
	- Exécuter en pas-à-pas l'instruction qui suit (`collect()`).
	- Visualiser de nouveau le contenu de la console Java et  des écrans *Jobs* et *Stages* de la SparkUI
	- Finaliser l'exécution de l'application
	- **Qu'avez-vous constaté ? Quelle propriété d'un RDD explique ce comportement ?**
	  > Instanciation paresseuse : le .collect() lance l'évaluation des transformations précédentes 

### Compilation Maven et exécution distribuée ###
- Créer sous Eclipse une *Run Configuration*  de type *Maven  Build*
	- Nom : `spark-batch mvn assembly single`
	- Base directory : le Workspace spark-batch
	- Goals : `compile assembly:single`
	- La lancer pour compiler l'application sous la forme d'un *fat Jar*
		- Rafraîchir le sous-dossier *target/* du navigateur de fichier Eclipse avec F5 et constater sa création

-  Créer sur la VM cluster dans le dossier home de l'utilisateur *mapr* un dossier de travail :
```
mkdir -p ~/formation/spark-batch
cd !$
```
- Transférer dans ce dossier le fichier Jar créé par la compilation
- L'exécuter avec spark-submit en mode local et vérifier la bonne exécution :
```
cd /usr/hdp/current/spark2-client
```
```
./bin/spark-submit --class formation.sparkbatch.BatchApp --master local[1] /home/maria_dev/tp-spark-batch-0.0.1-SNAPSHOT-jar-with-dependencies.jar
```

- **Quelle commande utiliser pour un test en mode YARN client ?** La tester.
	- *Tip  : se référer au TP précédent*
```
cd /usr/hdp/current/spark2-client
```
```
./bin/spark-submit --deploy-mode client --class formation.sparkbatch.BatchApp --master local[1] /home/maria_dev/tp-spark-batch-0.0.1-SNAPSHOT-jar-with-dependencies.jar
```

### Adaptation de l'application

> - Commenter le code de `BatchApp.main()` proposé en exemple
> - Isoler toutes les modifications qui suivent dans la classe `OpRDD`.

Modifier l'application pour implémenter les évolutions suivantes et répondre aux questions. 
Tester les évolutions d'abord localement .
- Charger dans un RDD le fichier `01-Gerer-Cluster-Dev\data\PERSONNE.csv`
	- *Tip : utiliser la méthode `jsc.textFile()`*
	- *Tip :  pour un test local sur le poste de développement via Eclipse, préfixer le chemin avec file:///C:/...., avec des slashes comme séparateurs*.
	- *Tip : Variabiliser ce chemin d'accès pour l'adapter aux tests en mode distribué*
- Calculer combien de ligne contient le fichier chargé
	- *Tip : se référer à la Javadoc de `JavaRDD` et [la documentation des transformations et actions](https://spark.apache.org/docs/latest/rdd-programming-guide.html#transformations)*
- Afficher dans la console les 10 premières lignes du fichier
- Filtrer la première ligne d'en-tête
- Créer une classe Java `Personne` contenant les mêmes champs que le fichier `PERSONNE.csv`. Parser les lignes du RDD Personne vers un `JavaRDD<Personne>` via la méthode `map()`.
    - *Tip utiliser la méthode `String.split()` pour découper la ligne du fichier au niveau d'un délimiteur*
	- *Tip la classe doit implémenter l'interface `Serializable` pour qu'elle puisse être utilisée pour typer un RDD sans erreur à l'exécution*
	- *Tip : une fois la classe `Personne` créée avec ses champs, utiliser le menu de l'IDE Eclipse *clic-droit sur la classe > Generate constructor using fields...* pour générer le constructeur à utiliser dans le `map()`
- Extraire la donnée SCORECREDIT dans un nouvel RDD
	- *Tip : utiliser la méthode `mapToDouble()`*
- **Quelle est la moyenne de SCORECREDIT** ?
  -  *Tip : Tester l'utilisation de la méthode [`mean()`](https://spark.apache.org/docs/latest/api/java/org/apache/spark/rdd/DoubleRDDFunctions.html). La  méthode [`reduce()`](https://spark.apache.org/docs/latest/api/java/org/apache/spark/api/java/JavaRDDLike.html#reduce-org.apache.spark.api.java.function.Function2-) pourrait également être utilisée.*
  >MEAN SCORECREDIT : 0.49553000000000014
  >MEAN SCORECREDIT WITH REDUCE: 0.49553000000000025

- **Calculer le nombre de personnes ayant score supérieur ou égal à la moyenne**
	> Nb of persons with scorecredit superior to the average : 490

- **Déterminer les 2 plus grands scores**
     - *Tip : Réaliser dans l'ordre : un `map()` pour extraire le score, un `distinct()` pour garder des valeurs uniques, un `sortBy()` pour trier par score, un `take()` pour conserver les 2 plus grands scores.*
   >Two highest scorescredit :
   > 1.0
   > 0.99


- Adapter le chemin de l'application pour tester sur la VM cluster l'application en mode distribué. Le fichier accédé est celui copié dans un TP précédemment sur MapR-FS dans `/user/mapr/formation/data/PERSONNE.csv`
	- *Tip : un chemin sans préfixe file:// ou avec le préfixe hdfs://* accède à MapR-FS

- Soumettre  les  réponses via git
```
git add .
git commit –m "réponses 04-Spark-Batch : RDD
git push
```

- Ouvrir un ticket GitHub nommé  *04-Spark-Batch : RDD* sur son dépôt et l'assigner au formateur pour demander une revue

## Manipulation de DataFrames	

> Isoler toutes les modifications qui suivent dans la classe `OpDF` .

###  Manipulation de fichiers

- Charger les fichiers `PERSONNE.csv`, `CONTRAT.csv`et `OFFRE.csv` dans des DataFrames.
	- *Tip : utiliser la méthode `SparkSession.read().csv()*
	- *Tip : penser à configurer le parser CSV avec la méthode `option()` et les clés/valeurs [correspondantes](https://spark.apache.org/docs/latest/api/java/org/apache/spark/sql/DataFrameReader.html#csv-java.lang.String...-), plus spécifiquement celle indiquant qu'il y a une entête dans le fichier.*

* Afficher avec `printSchema()` les schémas auto-détectés pour les jeux de données, et vérifier leur conformité

* Afficher les 5 premières personnes dans la console
   - *Tip : la méthode `takeAsList()` peut aider*

- **Combien de personnes contiennent dans leur Secteur d'activité le mot clé `Fabrication` ?**
	- *Tip La fonction statique `col()` peut importée avec `import static org.apache.spark.sql.functions.col;`.*
	- *Tip : utiliser la méthode [Column.contains()](https://spark.apache.org/docs/latest/api/java/org/apache/spark/sql/Column.html#contains-java.lang.Object-)*.
  > Nb of persons  working in "Fabrication" field : 275
### Manipulation de tables Hive

- Charge dans des DataFrames les tables Hive `PERSONNE`, `OFFRE`, `CONTRAT` de la base de données `FORMATION` créées dans le TP 02.
	- *Tip : ne pas oublier d'activer le support de Hive sur la session Spark avec `enableHiveSupport()`*
- Renommer la colonne du DataFrame `boactif` en `boactive`
- Supprimer la colonne du DataFrame `cdtarif`
- **Combien d'offres actives contiennent dans leur nom le mot clé `architecture` ?**
  > Nb of active architecture offers: 18

- Afficher les 10 contrats souscris le plus récemment

- Concaténer les colonnes `cdfamprof` et `cdsectactiv` du DataFrame Personne dans une nouvelle colonne  
	- *Tip : afin de profiter de la génération de code Tungsten éviter le recours à la méthode `map()` de transformation arbitraire, et préférer les [fonctions SQL](https://spark.apache.org/docs/latest/api/java/org/apache/spark/sql/functions.html)*

- **Quelle est la moyenne de score de crédit des personnes ayant souscrit à une offre lancée courant 2017 ?**
  > 0.43636363636363623

- Sauver les identifiants et date de naissance des personnes ayant souscrit à une offre contenant dans le libellé `open` (quelle que soit la casse) dans la table `formation.PERSONNE_OPEN` au format Parquet
- Vérifier avec Hue que le contenu de cette  table Hive `formation.PERSONNE_OPEN` peut être accédée

### Adaptations avancées (optionnel)

- Créer une nouvelle table `formation.CONTRATS_OPEN_RENEW` qui contient les contrats de ces personnes, mis à jour de la façon suivante :
	- `DTFIN` des contrats existants est valorisée à la date du jour
	- Un nouveau contrat est créé avec `DTSOUS = DTEFFET` = la date du jour, et `DTFIN` est repoussée à 1 an

- Créer une nouvelle table `formation.CONTRATS_STATS` qui contient l'identifiant et le nom de chaque client ainsi que son nombre de contrats.
	
- Créer une [UDF](https://spark.apache.org/docs/latest/sql-programming-guide.html#udf-registration-moved-to-sqlcontextudf-java--scala) nommée `year()` qui extrait l'année d'une date, et l'appliquer sur la table `OFFRE` pour crééer une nouvelle colonne `dt_ouverture_annee`

### Manipulation de fichiers JSON (optionnel)

- Sauver le contenu de la table Hive `formation.personnes_open`  précédemment créée sous la forme d'un fichier MapR-FS au format JSON Lines. L'intégrer à votre repository pour le soumettre au formateur.

- Recharger avec `SparkSession.json()` le fichier dans un DataFrame.
- **Combien de personnes issues de ce fichier ont plus de 20 ans ?**

- Soumettre  les  réponses via git
```
git add .
git commit –m "réponses 04-Spark-Batch : DataFrames
git push
```

- Ouvrir un ticket GitHub nommé  *04-Spark-Batch : DataFrames* sur son dépôt et l'assigner au formateur pour demander une revue

## Manipulation de Dataset

> Isoler toutes les modifications qui suivent dans la classe `OpDS` .

- Charger le fichier `personne.csv` dans un objet `Dataset<Personne>` qui réutilise la classe `Personne` créée précédemment.
   - *Tip : le schéma peut être spécifié avec la méthode `schema()` du Reader, et créé avec ` new StructType().add("attribut", "type", null)`*

- **Que se passe-t-il si le fichier CSV chargé contient une ligne avec un champ `SCORECREDIT` qui n'est pas un nombre ?**
   > Exception : mis à null, or null impossible pour double.
   > Exception in thread "main" java.lang.NullPointerException: Null value appeared in non-nullable field

- **Combien de personnes contiennent dans leur Secteur d'activité le mot clé `Fabrication` ?**
   > Nb of persons working in "Fabrication" field: 275

- **Quelle est la différence syntaxique pour réaliser ce calcul déjà réalisé avec un DataFrame ?**
   > 	 .filter(p -> p.getCdSectActiv().contains("Fabrication"))
   > Utilisation de l'attribut  de Personne au lieu des colonnes : typage fort

- **Combien de personnes ont un Secteur d'activité qui contient exactement  4 mots ?**
	- *Tip : cette transformation bénéficie du typage fort des Datasets*
	> Nb of persons working in a field containing more than 4 words: 167


- Soumettre  les  réponses via git
```
git add .
git commit –m "réponses 04-Spark-Batch : Datasets
git push
```

- Ouvrir un ticket GitHub nommé  *04-Spark-Batch : Datasets* sur son dépôt et l'assigner au formateur pour demander une revue

## Manipulation SQL

- Implémenter via Spark SQL une requête qui affiche le libellé de l'offre la plus vendue, à partir des données issues des tables Hive
	- *Tip : cette requête à été implémentée via Hive en TP 2*
>Future-proofed actuating circuit

- Soumettre  les  réponses via git
```
git add .
git commit –m "réponses 04-Spark-Batch : SQL
git push
```

 - Ouvrir un ticket GitHub nommé  *04-Spark-Batch : SQL* sur son dépôt et l'assigner au formateur pour demander une revue

