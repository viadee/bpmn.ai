package de.viadee.ki.sparkimporter.util.arguments;

import com.beust.jcommander.Parameter;
import de.viadee.ki.sparkimporter.exceptions.FaultyConfigurationException;
import de.viadee.ki.sparkimporter.runner.SparkRunner;
import de.viadee.ki.sparkimporter.runner.config.SparkRunnerConfig;
import de.viadee.ki.sparkimporter.util.SparkImporterVariables;
import org.apache.spark.sql.SaveMode;

/**
 * Configures command line parameters of the KAfka import application.
 */
public class KafkaProcessingArguments extends AbstractArguments {

	private static KafkaProcessingArguments sparkImporterArguments = null;

	@Parameter(names = { "--file-source",
			"-fs" }, required = true, description = "Directory in which Kafka Streams have been stored in as parquet files.")
	private String fileSource;

	@Parameter(names = { "--file-destination",
			"-fd" }, required = true, description = "The name of the target folder, where the resulting csv files are being stored, i.e. the data mining table.")
	private String fileDestination;

	@Parameter(names = { "--delimiter",
			"-d" }, required = true, description = "Character or string that should separate the fields in the resulting CSV file such as [ ;,  | or ||| ]. Please make sure that these are not contained in your data.")
	private String delimiter;

	@Parameter(names = { "--revision-count", "-rc" }, description = "Boolean toggle to enable the counting of changes "
			+ "to a variable. It results in a number of columns named <VARIABLE_NAME>_rev.", arity = 1)
	private boolean revisionCount = true;

	@Parameter(names = { "--step-results",
			"-sr" }, description = "Should intermediate results be written into CSV files?", arity = 1)
	private boolean writeStepResultsToCSV = false;

	@Parameter(names = { "--working-directory",
			"-wd" }, required = false, description = "Folder where the configuration files are stored or should be stored.")
	private String workingDirectory = "./";

	@Parameter(names = { "--log-directory",
			"-ld" }, required = false, description = "Folder where the log files should be stored.")
	private String logDirectory = "./";

	@Parameter(names = { "--dev-type-cast-check",
			"-devtcc" }, required = false, description = "Development feature: Check for type casting errors of columns.", arity = 1)
	private boolean devTypeCastCheckEnabled = false;

	@Parameter(names = { "--dev-process-state-column-workaround",
			"-devpscw" }, required = false, description = "Development feature: If the process state column is empty in source data (e.g. due to an older Camunda version) the matching is done on variable name column instead. Only works if data level is process!", arity = 1)
	private boolean devProcessStateColumnWorkaroundEnabled = false;

	@Parameter(names = { "--save-mode",
			"-sm" }, required = false, description = "Should the result be appended to the destination or should it be overwritten?")
	private String saveMode = SparkImporterVariables.SAVE_MODE_APPEND;

	@Parameter(names = { "--output-format",
			"-of" }, required = false, description = "In which format should the result be written (parquet or csv)?")
	private String outputFormat = SparkImporterVariables.OUTPUT_FORMAT_PARQUET;

	@Parameter(names = { "--data-level",
			"-dl" }, required = false, description = "Which level sjould the resulting data have. It can be process or activity.")
	private String dataLevel = SparkImporterVariables.DATA_LEVEL_PROCESS;

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

	private boolean isRevisionCount() {
		return revisionCount;
	}

	private String getFileSource() {
		return fileSource;
	}

	private String getDelimiter() {
		return delimiter;
	}

	private String getFileDestination() {
		return fileDestination;
	}

	private boolean isWriteStepResultsToCSV() {
		return writeStepResultsToCSV;
	}

	private String getWorkingDirectory() {
		return workingDirectory;
	}

	private String getLogDirectory() {
		return logDirectory;
	}

	private String getSaveMode() {
		return saveMode;
	}

	private String getOutputFormat() {
		return outputFormat;
	}

	private boolean isDevTypeCastCheckEnabled() {
		return devTypeCastCheckEnabled;
	}

	private boolean isDevProcessStateColumnWorkaroundEnabled() {
		return devProcessStateColumnWorkaroundEnabled;
	}

	private String getDataLevel() {
		return dataLevel;
	}

	private String getProcessDefinitionFilterId() {
		return processDefinitionId;
	}

	private String getOutputDelimiter() {
		return outputDelimiter;
	}

	private void setOutputDelimiter(String outputDelimiter) {
		this.outputDelimiter = outputDelimiter;
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

	public SparkRunnerConfig createOrUpdateSparkRunnerConfig(SparkRunnerConfig config) {
		if(config == null) {
			config = new SparkRunnerConfig();
		}

		config.setRunningMode(SparkRunner.RUNNING_MODE.KAFKA_PROCESSING);
		config.setSourceFolder(this.getFileSource());
		config.setTargetFolder(this.getFileDestination());
		config.setWorkingDirectory(this.getWorkingDirectory());
		config.setLogDirectory(this.getLogDirectory());
		config.setOutputFormat(this.getOutputFormat());
		config.setDevTypeCastCheckEnabled(this.isDevTypeCastCheckEnabled());
		config.setDevProcessStateColumnWorkaroundEnabled(this.isDevProcessStateColumnWorkaroundEnabled());
		config.setRevCountEnabled(this.isRevisionCount());
		config.setSaveMode(this.getSaveMode() == SparkImporterVariables.SAVE_MODE_APPEND ? SaveMode.Append : SaveMode.Overwrite);
		config.setProcessFilterDefinitionId(this.getProcessDefinitionFilterId());
		config.setDataLevel(this.getDataLevel());
		config.setWriteStepResultsIntoFile(this.isWriteStepResultsToCSV());
		config.setOutputDelimiter(this.getOutputDelimiter());

		validateConfig(config);

		return config;
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
