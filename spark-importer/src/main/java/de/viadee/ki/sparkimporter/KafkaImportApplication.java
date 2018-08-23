package de.viadee.ki.sparkimporter;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;
import de.viadee.ki.sparkimporter.runner.KafkaImportRunner;
import de.viadee.ki.sparkimporter.util.SparkImporterKafkaImportArguments;
import de.viadee.ki.sparkimporter.util.SparkImporterLogger;
import de.viadee.ki.sparkimporter.util.SparkImporterUtils;
import de.viadee.ki.sparkimporter.util.SparkImporterVariables;
import org.apache.commons.io.FileUtils;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

public class KafkaImportApplication {

	private static final Logger LOG = LoggerFactory.getLogger(KafkaImportApplication.class);
	public static SparkImporterKafkaImportArguments ARGS;

	public static void main(String[] arguments) {

		final long startMillis = System.currentTimeMillis();

		ARGS = SparkImporterKafkaImportArguments.getInstance();

		// instantiate JCommander
		// Use JCommander for flexible usage of Parameters
		final JCommander jCommander = JCommander.newBuilder().addObject(SparkImporterKafkaImportArguments.getInstance()).build();
		try {
			jCommander.parse(arguments);
		} catch (final ParameterException e) {
			LOG.error("Parsing of parameters failed. Error message: " + e.getMessage());
			jCommander.usage();
			System.exit(1);
		}

		//workaround to overcome the issue that different Application argument classes are used but we need the target folder for the result steps
		SparkImporterVariables.setTargetFolder(ARGS.getFileDestination());
		SparkImporterUtils.setWorkingDirectory(ARGS.getWorkingDirectory());
		SparkImporterLogger.setLogDirectory(ARGS.getLogDirectory());

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
