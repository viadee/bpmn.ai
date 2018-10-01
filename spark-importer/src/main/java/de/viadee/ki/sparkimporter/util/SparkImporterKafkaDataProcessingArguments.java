package de.viadee.ki.sparkimporter.util;

import com.beust.jcommander.Parameter;

/**
 * Configures command line parameters of the KAfka import application.
 */
public class SparkImporterKafkaDataProcessingArguments {

	private static SparkImporterKafkaDataProcessingArguments sparkImporterArguments = null;

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

	/**
	 * Singleton.
	 */
	private SparkImporterKafkaDataProcessingArguments() {
	}

	public boolean isRevisionCount() {
		return revisionCount;
	}

	public String getFileSource() {
		return fileSource;
	}

	public String getDelimiter() {
		return delimiter;
	}

	public String getFileDestination() {
		return fileDestination;
	}

	public boolean isWriteStepResultsToCSV() {
		return writeStepResultsToCSV;
	}

	public String getWorkingDirectory() {
		return workingDirectory;
	}

	public String getLogDirectory() {
		return logDirectory;
	}

	public String getSaveMode() {
		return saveMode;
	}

	public String getOutputFormat() {
		return outputFormat;
	}

	public boolean isDevTypeCastCheckEnabled() {
		return devTypeCastCheckEnabled;
	}

	public String getDataLevel() {
		return dataLevel;
	}
	
	public String getFilter() {
		return processDefinitionId;
	}

	/**
	 * @return SparkImporterKafkaDataProcessingArguments instance
	 */
	public static SparkImporterKafkaDataProcessingArguments getInstance() {
		if (sparkImporterArguments == null) {
			sparkImporterArguments = new SparkImporterKafkaDataProcessingArguments();
		}
		return sparkImporterArguments;
	}

	@Override
	public String toString() {
		return "SparkImporterKafkaDataProcessingArguments{" + "fileSource='" + fileSource + '\'' + ", delimiter='" + delimiter
				+ '\'' + ", fileDestination='" + fileDestination + '\'' + ", revisionCount=" + revisionCount
				+ '\'' + ", writeStepResultsToCSV='" + writeStepResultsToCSV
				+ '\'' + ", workingDirectory=" + workingDirectory
				+ '\'' + ", devTypeCastCheckEnabled=" + devTypeCastCheckEnabled
				+ '\'' + ", dataLevel=" + dataLevel
				+ '\'' + ", outputFormat=" + outputFormat
				+ '\'' + ", saveMode=" + saveMode
				+ '\'' + ", logDirectory=" + logDirectory + '}';
	}
}
