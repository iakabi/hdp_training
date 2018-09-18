
# Exercices


L'application Spark d'exemple **SparkPi** permet de produire une estimation du nombre Pi via la [formule de Leibniz](https://en.wikipedia.org/wiki/Leibniz_formula_for_%CF%80). Il s'agit d'une somme de 0 à N paramétrée. La somme correspond à des opérations **map** (calcul de chaque éléments) et **reduce** (la somme en elle-même) parallélisables sur un cluster Spark.

## Première exécution

- Dans une console SSH, lancer l'application via la commande `spark-submit` :

```cd /usr/hdp/current/spark2-client```
```./bin/spark-submit --class org.apache.spark.examples.SparkPi --master yarn --executor-memory 512m examples/jars/spark-examples*.jar 10```

- Constater l'estimation de Pi

- Relancer l'application avec une estimation plus précise et plus longue : 

```./bin/spark-submit --class org.apache.spark.examples.SparkPi --master yarn --executor-memory 512m examples/jars/spark-examples*.jar 100000```

> Pour la suite du TP :
> - relancer l'application si jamais elle s'arrête toute seule en fin de calcul.
> - Se référer aux documentations d'exécution d'application Spark :
>	  - https://spark.apache.org/docs/latest/submitting-applications.html
>   - https://spark.apache.org/docs/latest/running-on-yarn.html

- Ouvrir la console MCS, menu *Services*
- Cliquer sur *Resource Manager* pour ouvrir l'UI du Resource Manager
- Constater que l'application Spark lancée apparait

- **Combien de RAM et coeurs ont été allouées au cluster via YARN ?**
> 3 coeurs / 3000 MB de RAM 

- **Combien de containers YARN sont alloués à l'application ?**
  - *Tip : les containers d'une application sont visibles dans le sous-menu "Attempt" (tentative d'exécution) de l'application*
> 3 containers
- **Combien de coeurs l'application consomme-t-elle en régime de croisière ? Pourquoi ?**
> 3, un par container

- **Avec quel utilisateur YARN exécute l'application ? Comment l'utilisateur pourrait être changé ?**
> maria_dev. Lancer le job avec un autre user.


- Ouvrir l'UI de supervision de l'application Spark : dans la liste des applications de l'écran principal de l'UI du Resource Manager, cliquer sur le lien *Application Master* 

- **Combien d'Executors Spark sont alloués à l'application ?**
> 2 executor et un driver 

- **Le nombre d'Executors est-il différent du nombre de containers YARN ? Pourquoi ?**
> Chaque exécuteur et le driver se lance dans un container séparé : cela fait bien 3 containers au total

- Interrompre l’exécution de l'application dans la console avec Ctrl-C. Constater la libération des ressources dans l'UI du Resource Manager.

- L'ApplicationMaster est arrêtée et la console Spark UI n'est plus disponible. Constater que le lien vers elle est remplacée par un lien vers le Spark History Server qui a archivé les événéments d'exécution de l'application.

## Deuxième exécution : augmentation de la demande de ressources

- Utiliser les paramètres `--num-executors`,  `--executor-cores` et `--deploy-mode cluster` pour lancer l'application avec 4 Executors avec 1 coeur affecté à chacun, en mode YARN Cluster.
```./bin/spark-submit --class org.apache.spark.examples.SparkPi --master yarn --executor-memory 512m --num-executors 4 --executor-cores 1 --deploy-mode cluster  examples/jars/spark-examples*.jar 100000```
- Vérifier dans l'UI du Resource Manager et l'UI Spark que la demande d'allocation a bien été prise en compte

- Visualiser les logs des différents containers YARN :
	- Dans l'UI du Resource Manager, un lien vers les logs de l'Application Master est présent à côté de l'*attempt*
	- Dans l'UI du Resource Manager, dans le sous-écran lié à l'*attempt*, un lien est présent à côté de chaque Container correspondant à un Executor
	- Dans l'UI Spark,  les mêmes logs peuvent être accédées via les liens dans l'écran *Executor*

- Visualiser les logs via la commande `yarn`. Il s'agit d'une contaténation de toutes les logs des containers YARN.
```
yarn application -list
yarn logs -applicationId <INDIQUER_L'APPLICATION_ID>
```

- Visualiser (via Hue ou en ligne de commande via le montage POSIX) les mêmes logs sur MapR-FS dans la sous-arborescence `/tmp/logs/mapr/logs/[APPLICATION_ID]`

- Interrompre l’exécution de l'application dans la console avec Ctrl-C. **L'application YARN est-elle effectivement arrêtée ? Pourquoi ?**
> Non. Cluster mode = driver inside cluster and not in the spark-submit process.

- Arrêter l'application avec la commande `yarn`
```
yarn application -list
yarn application -kill <INDIQUER_L'APPLICATION_ID>
```

## Troisième exécution :  contention

- Lancer 2 fois l'application simultanément avec les mêmes paramètres que précédemment.
- **Que se passe-t-il dans l'UI du Resource Manager ?**
> Une application running et l'autre en attente

- Tuer les 2 application depuis le Job Browser de Hue

## Hive

- Depuis Hue, lancer une requête Hive de type `SELECT * FROM`.

- **Que se passe-t-il dans l'UI du Resource Manager ? A votre avis pourquoi ?**
> Rien. Requête simple, ne faisant que lire  dans le HDFS (pas de traitement), n'utilise donc pas Tez et n'apparait donc pas dans le ResourceManager

- Lancer une requête Hive plus complexe.
```
SELECT o.libelle 
FROM formation.offre as o, 
	(SELECT c.idoffre, count(c.idoffre) AS nb 
	FROM formation.contrat as c
	GROUP BY c.idoffre
	ORDER BY nb DESC
	LIMIT 100) as c
WHERE o.idoffre = c.idoffre
```
- **Que se passe-t-il dans l'UI du Resource Manager ?** (applications, containers, ressources utilisées...)
Application Tez : 1 container / 1 coeur / 500Mb de RAM

## Demande de revue
- Soumettre  les  réponses via git
```
git add .
git commit –m "réponses 03-Spark-sur-YARN"
git push
```

- Ouvrir un ticket GitHub nommé  *03-Spark-sur-YARN* sur son dépôt et l'assigner au formateur pour demander une revue
