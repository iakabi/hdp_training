package formation.sparkbatch;

import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.sql.SparkSession;

public class OpRDD {
	
	public static void run(SparkSession spark, JavaSparkContext jsc) throws Exception {
		System.out.println("\n*********** RDD ***********");

		String local_path = "file:///C:/Users/ImanAKABI/FormationHdp/Formation-Dev-MapR/01-Gerer-Cluster-Dev/data/PERSONNE.csv";
		String hdfs_path = "hdfs:///user/maria_dev/formation/data/PERSONNE.csv";

		JavaRDD<String> personsStringRDD = jsc.textFile(hdfs_path);
		
		//delete header
		Function2 removeHeader= new Function2<Integer, Iterator<String>, Iterator<String>>(){
		    @Override
		    public Iterator<String> call(Integer ind, Iterator<String> iterator) throws Exception {
		        if(ind==0 && iterator.hasNext()){
		            iterator.next();
		            return iterator;
		        }else
		            return iterator;
		    }
		};
		personsStringRDD = personsStringRDD.mapPartitionsWithIndex(removeHeader, false);

		//print 10 first persons
		System.out.println("*********** 10 first persons ***********");
		JavaRDD<String> ten_first_persons = jsc.parallelize(personsStringRDD.take(10));
		ten_first_persons.collect().forEach(e -> System.out.println(e));
		System.out.println();

		
		//create JavaRDD<Personne>
		JavaRDD<Personne> personnesRDD = personsStringRDD.map(p -> {
			String[] attributes = p.split(",");
			return new Personne(Integer.parseInt(attributes[0]), attributes[1], attributes[2], attributes[3], attributes[4], attributes[5],attributes[6], 
					attributes[7], attributes[8],attributes[9], Double.parseDouble(attributes[10]));
		});
		
		
		//compute mean scorecredit
		JavaDoubleRDD allScoreCredits = personnesRDD.mapToDouble(p -> p.getScoreCredit());
		System.out.println("*********** Mean SCORECREDIT ***********");
		System.out.println("MEAN SCORECREDIT : "+allScoreCredits.mean());
        double mean = allScoreCredits.reduce((a,b) -> a+b)/allScoreCredits.count(); 
		System.out.println("MEAN SCORECREDIT WITH REDUCE: "+ mean);
		System.out.println();

		
		//compute nb of persons with scorecredit superior to the average
		JavaDoubleRDD highScores = allScoreCredits.filter(s -> s >= mean);
		System.out.println("*********** Persons with scorecredit superior to the average ***********");
		System.out.println("Nb of persons with scorecredit superior to the average : " + highScores.count());
		System.out.println();
		
		
		//find two highest scorecredit (no sort method for JavaDoubleRDD)		
		System.out.println("*********** Two highest scorescredit ***********");
		List<Double> highestScores = allScoreCredits.distinct().takeOrdered(2, Comparator.reverseOrder()); //distinct destroy orders
		highestScores.forEach(s -> System.out.println(s));

		System.out.println("*********** END RDD ***********\n");
	}
}
