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

	@Parameter(names = { "--config-path",
			"-cp" }, required = false, description = "Folder where the configuration files are stored or should be stored.")
	private String configurationPath = "./";

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

	public String getConfigurationPath() {
		return configurationPath;
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
				+ '\'' + ", configurationPath=" + configurationPath + '}';
	}
}
