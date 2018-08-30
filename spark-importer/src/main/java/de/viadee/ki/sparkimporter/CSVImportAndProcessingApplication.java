package de.viadee.ki.sparkimporter;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;
import de.viadee.ki.sparkimporter.processing.aggregation.AllButEmptyStringAggregationFunction;
import de.viadee.ki.sparkimporter.processing.aggregation.ProcessStatesAggregationFunction;
import de.viadee.ki.sparkimporter.runner.CSVImportAndProcessingRunner;
import de.viadee.ki.sparkimporter.util.SparkImporterArguments;
import de.viadee.ki.sparkimporter.util.SparkImporterLogger;
import de.viadee.ki.sparkimporter.util.SparkImporterUtils;
import de.viadee.ki.sparkimporter.util.SparkImporterVariables;
import org.apache.commons.io.FileUtils;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

public class CSVImportAndProcessingApplication {

	private static final Logger LOG = LoggerFactory.getLogger(CSVImportAndProcessingApplication.class);
	public static SparkImporterArguments ARGS;

	public static void main(String[] arguments) {
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

		//workaround to overcome the issue that different Application argument classes are used but we need the target folder for the result steps
		SparkImporterVariables.setTargetFolder(ARGS.getFileDestination());
		SparkImporterVariables.setDevTypeCastCheckEnabled(ARGS.isDevTypeCastCheckEnabled());
		SparkImporterUtils.setWorkingDirectory(ARGS.getWorkingDirectory());
		SparkImporterLogger.setLogDirectory(ARGS.getLogDirectory());

		// SparkImporter code starts here

		// Delete destination files, required to avoid exception during runtime
		FileUtils.deleteQuietly(new File(ARGS.getFileDestination()));

		// configuration is being loaded from Environment (e.g. when using spark-submit)
		final SparkSession sparkSession = SparkSession.builder().getOrCreate();

		// register our own aggregation function
		sparkSession.udf().register("AllButEmptyString", new AllButEmptyStringAggregationFunction());
		sparkSession.udf().register("ProcessState", new ProcessStatesAggregationFunction());

		CSVImportAndProcessingRunner csvImportAndProcessingRunner = new CSVImportAndProcessingRunner();
		csvImportAndProcessingRunner.run(sparkSession);

		// Cleanup
		sparkSession.close();
	}

}
