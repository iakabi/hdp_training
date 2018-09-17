-- ##########################
-- ##### INITIALISATION #####
-- ##########################

-- DATABASE
CREATE DATABASE formation;
USE formation;

-- TABLE PERSONNE
CREATE EXTERNAL TABLE IF NOT EXISTS formation.personne (
	idpersonne INT, 
	nom STRING, 
	prenom STRING, 
	cdcivil STRING, 
	dtnaissance DATE, 
	cdsitufam STRING, 
	cdfamprof STRING, 
	cdsectactiv STRING, 
	anneesemploi INT, 
	email STRING, 
	scorecredit FLOAT)
COMMENT 'Personne details'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
TBLPROPERTIES ("skip.header.line.count"="1");

-- TABLE OFFRE
CREATE EXTERNAL TABLE IF NOT EXISTS formation.offre (
	idoffre INT,
	libelle STRING,
	dtouverture STRING,
	cdtarif INT,
	boactif BOOLEAN)
COMMENT 'OFFRE details'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
TBLPROPERTIES ("skip.header.line.count"="1");

-- TABLE CONTRAT
CREATE EXTERNAL TABLE IF NOT EXISTS formation.contrat (
	idcontrat INT,
	idpersonne INT, 
	dtsous DATE,
	dteffet DATE,
	dtfin DATE,
	idoffre INT)
COMMENT 'CONTRAT details'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
TBLPROPERTIES ("skip.header.line.count"="1");

-- LOAD DATA IN APPROPRIATE DATABASE
LOAD DATA INPATH  '/user/maria_dev/formation/data/hive/PERSONNE/PERSONNE.csv' OVERWRITE INTO TABLE formation.personne;
LOAD DATA INPATH  '/user/maria_dev/formation/data/hive/OFFRE/OFFRE.csv' OVERWRITE INTO TABLE formation.offre;
LOAD DATA INPATH  '/user/maria_dev/formation/data/hive/CONTRAT/CONTRAT.csv' OVERWRITE INTO TABLE formation.contrat;

-- SELECTS
SELECT * FROM formation.personne LIMIT 10;
SELECT * FROM formation.offre LIMIT 10;
SELECT * FROM formation.contrat LIMIT 10;

--DROPS
--DROP TABLE formation.personne;
--DROP TABLE formation.offre
--DROP TABLE formation.contrat



-- ###############
-- ##### HQL #####
-- ###############

-- 1) a) Depuis combien d'années en moyenne les personnes travaillent chez leur employeur?
SELECT AVG(anneesemploi) FROM formation.personne;
-- RESULT : 5.044

-- 1) b) Réexécuter la même requête de calcul, que constatez-vous en terme de temps d'exécution ? Pourquoi ?
-- Premier temps d'execution = 16s 267 (Tez view), Deuxième temps d'execution = 12s801. Data cache (data in memory instead of disk)

-- 2) Quel est le libellé de l'offre la plus vendue
SELECT o.libelle 
FROM formation.offre as o, 
	(SELECT c.idoffre, count(c.idoffre) AS nb 
	FROM formation.contrat as c
	GROUP BY c.idoffre
	ORDER BY nb DESC
	LIMIT 1) as c
WHERE o.idoffre = c.idoffre
-- RESULT : 

-- 3) Quelle est la date de la plus ancienne offre ?
SELECT TO_DATE(from_unixtime(UNIX_TIMESTAMP(dtouverture, 'yyyy/MM/dd'))) as dtouverture_date
FROM formation.offre
ORDER BY dtouverture_date ASC
LIMIT 1
-- RESULT : 2010-01-02

-- 4) Supprimer la table CONTRAT avec une requête SQL
DROP TABLE formation.contrat

-- 5) Le dossier /user/mapr/formation/data/hive/CONTRAT et son contenu sont-ils supprimés ?
-- Normalement non, mais la fonction LOAD a déjà supprimé le fichier du HDFS

-- 6)Recréer la table CONTRAT puis Créer en SQL une nouvelle table (interne) CONTRAT_SEQ contenant les données contrat au format SequenceFile
CREATE TABLE formation.contrat_seq STORED AS SEQUENCEFILE AS SELECT * FROM formation.contrat;

-- 7)Créer en SQL une nouvelle table (interne) CONTRAT_PARQUET contenant les données contrat au format Parquet
CREATE TABLE formation.contrat_parquet STORED AS PARQUET AS SELECT * FROM formation.contrat;

-- 8)Comparer en SSH via la commande cat -v le format des fichiers ainsi créés par Hive vs le format des fichiers le table CONTRAT
-- Sequence files store data in a binary format with a similar structure to CSV 
-- Parquet : compressed, efficient columnar data representation  

-- 9) Consulter la structure de la table PERSONNE telle que définie dans le Metastore Hive, via l'écran Hue Data Browser > Metastore Tables. Ajouter un commentaire de description
-- Structure de la table visible dans onglet Tables/DDL de Hive View. Modification du commentaire non possible via l'UI. Requête SQL suivante équivalente : 
ALTER TABLE formation.personne SET TBLPROPERTIES ('comment' = 'Personne informations')


-- 10) En SSH, avec le client ligne de commande Hive (commande hive) afficher la structure de la table PERSONNE avec la commande SQL DESCRIBE FORMATTED. Vérifier que le commentaire ajouté précédemment apparaît
--hive
--DESCRIBE FORMATTED formation.personne

-- Commentaire de la table PERSONNE a bien été mis à jour


-- 11) Utiliser le mode d'exécution de Hive scripté (commande hive -e) pour créer un fichier CSV sans-entête sur MapR-FS dans le dossier /user/mapr/formation/hive/LIBOFFRE contenant 2 champs
-- hive -e 'SELECT libelle, idoffre  FROM formation.offre ORDER BY libelle' | hadoop fs -put - /user/maria_dev/formation/data/hive/offre_csv_export.csv

