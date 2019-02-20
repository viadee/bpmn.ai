package de.viadee.ki.sparkimporter.util;

import com.beust.jcommander.Parameter;

/**
 * Configures command line parameters of the KAfka import application.
 */
public class SparkImporterKafkaImportArguments {

	private static SparkImporterKafkaImportArguments sparkImporterArguments = null;

	@Parameter(names = { "--kafka-broker",
			"-kb" }, required = true, description = "Server and port of Kafka broker to consume from")
	private String kafkaBroker;

	@Parameter(names = { "--file-destination",
			"-fd" }, required = true, description = "The name of the target folder, where the resulting parquet files are being stored.")
	private String fileDestination;

	@Parameter(names = { "--step-results",
			"-sr" }, description = "Should intermediate results be written into CSV files?", arity = 1)
	private boolean writeStepResultsToCSV = false;

	@Parameter(names = { "--batch-mode",
			"-bm" }, required = true, description = "Should application run in batch mode? It then stops after all pulled queues have returned zero entries at least once", arity = 1)
	private boolean batchMode = false;

	@Parameter(names = { "--working-directory",
			"-wd" }, required = false, description = "Folder where the configuration files are stored or should be stored.")
	private String workingDirectory = "./";

	@Parameter(names = { "--log-directory",
			"-ld" }, required = false, description = "Folder where the log files should be stored.")
	private String logDirectory = "./";

	@Parameter(names = { "--data-level",
			"-dl" }, required = false, description = "Which level sjould the resulting data have. It can be process or activity.")
	private String dataLevel = SparkImporterVariables.DATA_LEVEL_PROCESS;
	
	@Parameter(names = { "--process-filter",
	"-pf" }, required = false, description = "Execute pipeline for a specific processDefinitionId.")
	private String processDefinitionId = null;
	
	@Parameter(names = { "--output-format",
	"-of" }, required = false, description = "In which format should the result be written (parquet or csv)?")
	private String outputFormat = SparkImporterVariables.OUTPUT_FORMAT_PARQUET;

	/**
	 * Singleton.
	 */
	private SparkImporterKafkaImportArguments() {
	}


	public String getKafkaBroker() {
		return kafkaBroker;
	}

	public String getFileDestination() {
		return fileDestination;
	}

	public boolean isWriteStepResultsToCSV() {
		return writeStepResultsToCSV;
	}

	public boolean isBatchMode() {
		return batchMode;
	}

	public String getWorkingDirectory() {
		return workingDirectory;
	}

	public String getLogDirectory() {
		return logDirectory;
	}

	public String getDataLevel() {
		return dataLevel;
	}
	
	public String getProcessDefinitionFilterId() {
		return processDefinitionId;
	}
	
	public String getOutputFormat() {
		return outputFormat;
	}

	/**
	 * @return SparkImporterKafkaImportArguments instance
	 */
	public static SparkImporterKafkaImportArguments getInstance() {
		if (sparkImporterArguments == null) {
			sparkImporterArguments = new SparkImporterKafkaImportArguments();
		}
		return sparkImporterArguments;
	}

	@Override
	public String toString() {
		return "SparkImporterKafkaImportArguments{" + "kafkaBroker='" + kafkaBroker + '\'' + ", fileDestination='" + fileDestination
				+ '\'' + ", writeStepResultsToCSV=" + writeStepResultsToCSV + '}'
				+ '\'' + ", batchMode=" + batchMode
				+ '\'' + ", workingDirectory=" + workingDirectory
				+ '\'' + ", dataLavel=" + dataLevel
				+ '\'' + ", logDirectory=" + logDirectory + '}';
	}
}
