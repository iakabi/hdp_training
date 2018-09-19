package formation.sparkbatch;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

public class BatchApp {
	public static void main(String[] args) throws Exception {
		
		SparkSession spark = SparkSession.builder().appName("TP Spark Batch").enableHiveSupport().getOrCreate(); //distributed
		//SparkSession spark = SparkSession.builder().appName("TP Spark Batch").getOrCreate(); //local
		spark.sparkContext().setLogLevel("WARN");

		JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());

		List<String> list = Arrays.asList("hello", "world!");
		JavaRDD<String> rdd = jsc.parallelize(list);
		JavaRDD<String> upperRdd = rdd.map(e -> { System.out.println("in map for [" + e + "]"); return e.toUpperCase();});
		upperRdd.collect().forEach(e -> System.out.println(e));
		
		OpRDD.run(spark, jsc);
		OpDF.run(spark);
		OpDS.run(spark);
		OpSQL.run(spark);
		
		jsc.close();
		spark.stop();
	}
}
