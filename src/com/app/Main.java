package com.app;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.SparkSession;

public class Main {

	public static void main(String[] args) throws Exception {		
		SparkSession session = SparkSession.builder().appName("ABSTAT-spark").master(args[0]).getOrCreate();
		String dataset_file = args[1];
		String output_dir = args[2];
		String separator = args[3];
		
		JavaRDD<String> input = session.read().textFile(dataset_file).javaRDD();

		JavaRDD<String> csv = input.map(new Function<String, String>() {
			public String call(String s) {
				return s.replace("\t", separator);
			}});
		
		csv.coalesce(1).saveAsTextFile(output_dir);

	}
}
