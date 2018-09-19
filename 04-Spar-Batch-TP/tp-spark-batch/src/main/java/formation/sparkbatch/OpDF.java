package formation.sparkbatch;

import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

public class OpDF {
	public static void run(SparkSession spark) throws Exception {
		System.out.println("\n*********** DataFrame ***********");

		//FILES
		//create DataFrames
		String local_path_personne = "file:///C:/Users/ImanAKABI/FormationHdp/Formation-Dev-MapR/01-Gerer-Cluster-Dev/data/PERSONNE.csv";
		String local_path_offre = "file:///C:/Users/ImanAKABI/FormationHdp/Formation-Dev-MapR/01-Gerer-Cluster-Dev/data/OFFRE.csv";
		String local_path_contrat = "file:///C:/Users/ImanAKABI/FormationHdp/Formation-Dev-MapR/01-Gerer-Cluster-Dev/data/CONTRAT.csv";

		String remote_path_personne = "hdfs:///user/maria_dev/formation/data/PERSONNE.csv";
		String remote_path_offre = "hdfs:///user/maria_dev/formation/data/OFFRE.csv";
		String remote_path_contrat = "hdfs:///user/maria_dev/formation/data/CONTRAT.csv";

		Dataset<Row> dfPersonnes = spark.read()
				.option("inferSchema", "true")
				.option("header", "true")
				.csv(remote_path_personne);

		Dataset<Row> dfOffres = spark.read()
				.option("inferSchema", "true")
				.option("header", "true")
				.option("timestampFormat", "yyyy/MM/dd")
				.csv(remote_path_offre);

		Dataset<Row> dfContrats = spark.read()
				.option("inferSchema", "true")
				.option("header", "true")
				.csv(remote_path_contrat);

		dfPersonnes.printSchema();
		dfOffres.printSchema();
		dfContrats.printSchema();

		//print 5 first persons
		System.out.println("*********** 5 first persons ***********");
		dfPersonnes.takeAsList(5).forEach(e -> System.out.println(e));
		
		//print nb of persons working in "Fabrication" field
		System.out.println("*********** Persons working in \"Fabrication\" field ***********");
		long nbWorkersFabrication = dfPersonnes.filter(functions.col("CDSECTACTIV")
				.contains("Fabrication"))
				.count();
		System.out.println("Nb of persons working in \"Fabrication\" field: " + nbWorkersFabrication );

		//HIVE TABLES
		//load hive tables in dataframes
		dfPersonnes = spark.read().table("formation.personne");
		dfOffres = spark.read().table("formation.offre");
		dfContrats = spark.read().table("formation.contrat");

		//rename column and delete column
		System.out.println("*********** Rename column BOACTIF and delete column CDTARIF of Offre ***********");
		dfOffres = dfOffres.withColumnRenamed("BOACTIF", "BOACTIVE").drop("CDTARIF");
		dfOffres.printSchema();

		//print nb of active architecture offers
		System.out.println("*********** Active architecture offers ***********");
		long nbArchiActiveOffers = dfOffres
				.filter(functions.col("BOACTIF"))
				.filter(functions.col("LIBELLE").contains("architecture"))
				.count();
		System.out.println("Nb of active architecture offers: " + nbArchiActiveOffers );
		
		//print 10 most recent offers
		System.out.println("*********** 10 most recent offers ***********");
		List<Row> mostRecentOffers = dfOffres.sort(functions.desc("DTOUVERTURE")).takeAsList(10);
		mostRecentOffers.forEach(e -> System.out.println(e));
		
		//concat columns
		System.out.println("*********** Concat columns CDFAMPROF and CDSECTACTIV of Personne***********");
		Dataset<Row> dfConcat = dfPersonnes.withColumn("CONCAT CDFAMPROF CDSECTACTIV", functions.concat_ws(" - ",functions.col("CDFAMPROF"), functions.col("CDSECTACTIV")));
		dfConcat.printSchema();
		dfConcat.takeAsList(5).forEach(e -> System.out.println(e));
		
		//mean scorecredit person having a contract with an offer created on 2017 
		System.out.println("*********** Mean score credit of persons having a contract with an offer created on 2017***********");
		dfOffres.filter(functions.col("DTOUVERTURE").gt("2017-01-01"))
				.filter(functions.col("DTOUVERTURE").lt("2018-01-01"))
				.join(dfContrats, "IDOFFRE")
				.select(functions.col("IDPERSONNE"))
				.distinct()
				.join(dfPersonnes, "IDPERSONNE")
				.select(functions.mean("SCORECREDIT"))
				.show();
		
		//save in a new table id and date of birth of persons having a contract whose name contains word 'open'
		dfOffres.filter(functions.col("LIBELLE").rlike("(?i)open"))
				.join(dfContrats, "IDOFFRE")
				.select(functions.col("IDPERSONNE"))
				.distinct()
				.join(dfPersonnes, "IDPERSONNE")
				.select(functions.col("IDPERSONNE"), functions.col("DTNAISSANCE"))
				.write()
				.option("path", "hdfs://sandbox-hdp.hortonworks.com:8020/apps/hive/warehouse/formation.db/personne_open")
				.format("parquet")
				.saveAsTable("formation.personne_open");

		System.out.println("*********** End DataFrame ***********\n");
	}
}
