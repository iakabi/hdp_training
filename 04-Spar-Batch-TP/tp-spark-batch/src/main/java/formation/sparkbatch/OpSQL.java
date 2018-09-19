package formation.sparkbatch;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class OpSQL {

	public static void run(SparkSession spark) throws Exception {
		System.out.println("\n*********** Spark SQL ***********");

		//print most sold offer
		System.out.println("\n*********** Most sold offer ***********");
		Dataset<Row> dsLibelle = spark.sql("SELECT o.libelle " + 
				"FROM formation.offre as o, " + 
				"(SELECT c.idoffre, count(c.idoffre) AS nb " + 
					"FROM formation.contrat as c " + 
					"GROUP BY c.idoffre " + 
					"ORDER BY nb DESC " + 
					"LIMIT 1) as c " + 
				"WHERE o.idoffre = c.idoffre ");
		dsLibelle.foreach(e -> System.out.println(e));
		
		System.out.println("*********** END Spark SQL ***********\n");
	}
}
