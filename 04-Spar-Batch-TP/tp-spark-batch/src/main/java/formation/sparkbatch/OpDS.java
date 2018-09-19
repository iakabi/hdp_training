package formation.sparkbatch;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.*;
import org.apache.spark.sql.types.IntegerType;
public class OpDS {
	
	public static void run(SparkSession spark) throws Exception {
		System.out.println("\n*********** DataSet ***********");

		String local_path = "file:///C:/Users/ImanAKABI/FormationHdp/Formation-Dev-MapR/01-Gerer-Cluster-Dev/data/PERSONNE.csv";
		String hdfs_path = "hdfs:///user/maria_dev/formation/data/PERSONNE.csv";

		//schema
		StructType structType = new StructType()
				.add("idpersonne", DataTypes.IntegerType)
				.add("nom", DataTypes.StringType)
				.add("prenom", DataTypes.StringType)
				.add("cdCivil", DataTypes.StringType)
				.add("dtNaissance", DataTypes.StringType)
				.add("cdSituFam", DataTypes.StringType)
				.add("cdFamProf", DataTypes.StringType)
				.add("cdSectActiv", DataTypes.StringType)
				.add("anneesEmploi", DataTypes.StringType)
				.add("email", DataTypes.StringType)
				.add("scoreCredit", DataTypes.DoubleType);
		
		//create DataSet
		System.out.println("*********** Schema Person ***********");
		Dataset<Personne> dsPersonne = spark.read()
				.schema(structType)
				.option("header", true)
				.csv(hdfs_path)
				.as(Encoders.bean(Personne.class));
		dsPersonne.printSchema();
		dsPersonne.takeAsList(5).forEach(e -> System.out.println(e));
		
		
		//print nb of persons working in "Fabrication" field
		System.out.println("*********** Persons working in \"Fabrication\" field ***********");
		long nbWorkersFabrication = dsPersonne
				.filter(p -> p.getCdSectActiv().contains("Fabrication"))
				.count();
		System.out.println("Nb of persons working in \"Fabrication\" field: " + nbWorkersFabrication );

		//print nb of persons working in a field containing 4 words
		System.out.println("*********** Persons working in a field containing more than 4 words ***********");
		long nbWorkers4WordsField = dsPersonne
				.filter(p -> p.getCdSectActiv().split(" ").length == 4)
				.count();
		System.out.println("Nb of persons working in a field containing more than 4 words: " + nbWorkers4WordsField );

		System.out.println("*********** END DataSet ***********\n");
		System.out.println();
	}
	
}

