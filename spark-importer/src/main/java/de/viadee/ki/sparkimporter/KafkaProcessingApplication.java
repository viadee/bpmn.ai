package de.viadee.ki.sparkimporter;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;
import de.viadee.ki.sparkimporter.processing.aggregation.AllButEmptyStringAggregationFunction;
import de.viadee.ki.sparkimporter.processing.aggregation.ProcessStatesAggregationFunction;
import de.viadee.ki.sparkimporter.runner.KafkaProcessingRunner;
import de.viadee.ki.sparkimporter.util.SparkImporterKafkaDataProcessingArguments;
import de.viadee.ki.sparkimporter.util.SparkImporterVariables;
import org.apache.commons.io.FileUtils;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

public class KafkaProcessingApplication {

	private static final Logger LOG = LoggerFactory.getLogger(KafkaProcessingApplication.class);
	public static SparkImporterKafkaDataProcessingArguments ARGS;

	public static void main(String[] arguments) {

		ARGS = SparkImporterKafkaDataProcessingArguments.getInstance();

		// instantiate JCommander
		// Use JCommander for flexible usage of Parameters
		final JCommander jCommander = JCommander.newBuilder().addObject(SparkImporterKafkaDataProcessingArguments.getInstance()).build();
		try {
			jCommander.parse(arguments);
		} catch (final ParameterException e) {
			LOG.error("Parsing of parameters failed. Error message: " + e.getMessage());
			jCommander.usage();
			System.exit(1);
		}

		//workaround to overcome the issue that different Application argument classes are used but we need the target folder for the result steps
		SparkImporterVariables.setTargetFolder(ARGS.getFileDestination());

		final long startMillis = System.currentTimeMillis();

		// SparkImporter code starts here

		// Delete destination files, required to avoid exception during runtime
		FileUtils.deleteQuietly(new File(ARGS.getFileDestination()));

		// configuration is being loaded from Environment (e.g. when using spark-submit)
		final SparkSession sparkSession = SparkSession.builder().getOrCreate();

		// register our own aggregation function
		sparkSession.udf().register("AllButEmptyString", new AllButEmptyStringAggregationFunction());
		sparkSession.udf().register("ProcessState", new ProcessStatesAggregationFunction());

		KafkaProcessingRunner kafkaProcessingRunner = new KafkaProcessingRunner();
		kafkaProcessingRunner.run(sparkSession);

		// Cleanup
		sparkSession.close();

		final long endMillis = System.currentTimeMillis();

		LOG.info("Job ran for " + ((endMillis - startMillis) / 1000) + " seconds in total.");
	}

}