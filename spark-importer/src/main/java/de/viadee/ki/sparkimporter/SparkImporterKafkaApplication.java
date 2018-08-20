package de.viadee.ki.sparkimporter;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;
import de.viadee.ki.sparkimporter.runner.KafkaImportRunner;
import de.viadee.ki.sparkimporter.util.SparkImporterArguments;
import org.apache.commons.io.FileUtils;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

public class SparkImporterKafkaApplication {

	private static final Logger LOG = LoggerFactory.getLogger(SparkImporterKafkaApplication.class);
	public static SparkImporterArguments ARGS;

	public static void main(String[] arguments) {

		final long startMillis = System.currentTimeMillis();

		ARGS = SparkImporterArguments.getInstance();

		// instantiate JCommander
		// Use JCommander for flexible usage of Parameters
		final JCommander jCommander = JCommander.newBuilder().addObject(SparkImporterArguments.getInstance()).build();
		try {
			jCommander.parse(arguments);
		} catch (final ParameterException e) {
			LOG.error("Parsing of parameters failed. Error message: " + e.getMessage());
			jCommander.usage();
			System.exit(1);
		}

		// SparkImporter code starts here

		// Delete destination files, required to avoid exception during runtime
		FileUtils.deleteQuietly(new File(ARGS.getFileDestination()));

		// configuration is being loaded from Environment (e.g. when using spark-submit)
		final SparkSession sparkSession = SparkSession.builder().getOrCreate();

		KafkaImportRunner kafkaImportRunner = new KafkaImportRunner();
		kafkaImportRunner.run(sparkSession);

		// Cleanup
		sparkSession.close();

		final long endMillis = System.currentTimeMillis();

		LOG.info("Job ran for " + ((endMillis - startMillis) / 1000) + " seconds in total.");
	}

}
