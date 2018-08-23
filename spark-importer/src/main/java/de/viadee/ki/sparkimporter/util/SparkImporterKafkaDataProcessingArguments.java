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
				+ '\'' + ", logDirectory=" + logDirectory + '}';
	}
}
