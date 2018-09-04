package de.viadee.ki.sparkimporter;

import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SparkTestApplication {

	private static final Logger LOG = LoggerFactory.getLogger(SparkTestApplication.class);

	public static void main(String[] arguments) {
		// configuration is being loaded from Environment (e.g. when using spark-submit)
		final SparkSession sparkSession = SparkSession.builder().master("local[*]").appName("Test").getOrCreate();


		// spark stuff


		// Cleanup
		sparkSession.close();
	}

}
