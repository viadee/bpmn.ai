package de.viadee.ki.sparkimporter.util.arguments;

import com.beust.jcommander.Parameter;
import de.viadee.ki.sparkimporter.runner.SparkRunner;
import de.viadee.ki.sparkimporter.runner.config.SparkRunnerConfig;

/**
 * Configures command line parameters of the KAfka import application.
 */
public class KafkaProcessingArguments extends AbstractArguments {

	private static KafkaProcessingArguments sparkImporterArguments = null;

	@Parameter(names = { "--file-source",
			"-fs" }, required = true, description = "Directory in which Kafka Streams have been stored in as parquet files.")
	private String fileSource;

	@Parameter(names = { "--delimiter",
			"-d" }, required = true, description = "Character or string that should separate the fields in the resulting CSV file such as [ ;,  | or ||| ]. Please make sure that these are not contained in your data.")
	private String delimiter;

	@Parameter(names = { "--revision-count", "-rc" }, description = "Boolean toggle to enable the counting of changes "
			+ "to a variable. It results in a number of columns named <VARIABLE_NAME>_rev.", arity = 1)
	private boolean revisionCount = true;

	@Parameter(names = { "--dev-type-cast-check",
			"-devtcc" }, required = false, description = "Development feature: Check for type casting errors of columns.", arity = 1)
	private boolean devTypeCastCheckEnabled = false;

	@Parameter(names = { "--dev-process-state-column-workaround",
			"-devpscw" }, required = false, description = "Development feature: If the process state column is empty in source data (e.g. due to an older Camunda version) the matching is done on variable name column instead. Only works if data level is process!", arity = 1)
	private boolean devProcessStateColumnWorkaroundEnabled = false;

	@Parameter(names = { "--process-filter",
	"-pf" }, required = false, description = "Execute pipeline for a specific processDefinitionId.")
	private String processDefinitionId = null;

	@Parameter(names = { "--output-delimiter",
			"-od" }, required = false, description = "Character or string that separates fields such as [ ;,  | or ||| ] for the written csv file. Please make sure that these are not contained in your data.")
	private String outputDelimiter = "|";

	/**
	 * Singleton.
	 */
	private KafkaProcessingArguments() {
	}

	/**
	 * @return SparkImporterKafkaDataProcessingArguments instance
	 */
	public static KafkaProcessingArguments getInstance() {
		if (sparkImporterArguments == null) {
			sparkImporterArguments = new KafkaProcessingArguments();
		}
		return sparkImporterArguments;
	}

	@Override
	public void createOrUpdateSparkRunnerConfig(SparkRunnerConfig config) {
		super.createOrUpdateSparkRunnerConfig(config);

		config.setRunningMode(SparkRunner.RUNNING_MODE.KAFKA_PROCESSING);
		config.setSourceFolder(this.fileSource);
		config.setDevTypeCastCheckEnabled(this.devTypeCastCheckEnabled);
		config.setDevProcessStateColumnWorkaroundEnabled(this.devProcessStateColumnWorkaroundEnabled);
		config.setRevCountEnabled(this.revisionCount);
		config.setProcessFilterDefinitionId(this.processDefinitionId);
		config.setOutputDelimiter(this.outputDelimiter);

		validateConfig(config);
	}

	@Override
	public String toString() {
		return "KafkaProcessingArguments{" + "fileSource='" + fileSource
				+ '\'' + ", delimiter='" + delimiter
				+ '\'' + ", fileDestination='" + fileDestination
				+ '\'' + ", revisionCount=" + revisionCount
				+ '\'' + ", writeStepResultsToCSV='" + writeStepResultsToCSV
				+ '\'' + ", workingDirectory=" + workingDirectory
				+ '\'' + ", devTypeCastCheckEnabled=" + devTypeCastCheckEnabled
				+ '\'' + ", devProcessStateColumnWorkaroundEnabled=" + devProcessStateColumnWorkaroundEnabled
				+ '\'' + ", dataLevel=" + dataLevel
				+ '\'' + ", outputFormat=" + outputFormat
				+ '\'' + ", outputDelimiter='" + outputDelimiter
				+ '\'' + ", saveMode=" + saveMode
				+ '\'' + ", logDirectory=" + logDirectory + '}';
	}
}
