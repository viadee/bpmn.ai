package de.viadee.bpmnai.core.util.arguments;

import com.beust.jcommander.Parameter;
import de.viadee.bpmnai.core.runner.SparkRunner;
import de.viadee.bpmnai.core.runner.config.SparkRunnerConfig;

/**
 * Configures command line parameters of the KAfka import application.
 */
public class KafkaImportArguments extends AbstractArguments {

	private static KafkaImportArguments sparkImporterArguments = null;

	@Parameter(names = { "--kafka-broker",
			"-kb" }, required = true, description = "Server and port of Kafka broker to consume from")
	private String kafkaBroker;

	@Parameter(names = { "--batch-mode",
			"-bm" }, required = true, description = "Should application run in batch mode? It then stops after all pulled queues have returned zero entries at least once", arity = 1)
	private boolean batchMode = false;

	@Parameter(names = { "--process-filter",
	"-pf" }, required = false, description = "Execute pipeline for a specific processDefinitionId.")
	private String processDefinitionId = null;
	
	/**
	 * Singleton.
	 */
	private KafkaImportArguments() {
	}

	/**
	 * @return SparkImporterKafkaImportArguments instance
	 */
	public static KafkaImportArguments getInstance() {
		if (sparkImporterArguments == null) {
			sparkImporterArguments = new KafkaImportArguments();
		}
		return sparkImporterArguments;
	}

	@Override
	public void createOrUpdateSparkRunnerConfig(SparkRunnerConfig config) {
		super.createOrUpdateSparkRunnerConfig(config);

		config.setRunningMode(SparkRunner.RUNNING_MODE.KAFKA_IMPORT);
		config.setProcessFilterDefinitionId(this.processDefinitionId);
		config.setBatchMode(this.batchMode);
		config.setKafkaBroker(this.kafkaBroker);

		validateConfig(config);
	}

	@Override
	public String toString() {
		return "KafkaImportArguments{" + "kafkaBroker='" + kafkaBroker
				+ '\'' + ", fileDestination='" + fileDestination
				+ '\'' + ", writeStepResultsToCSV=" + writeStepResultsToCSV + '}'
				+ '\'' + ", batchMode=" + batchMode
				+ '\'' + ", workingDirectory=" + workingDirectory
				+ '\'' + ", dataLavel=" + dataLevel
				+ '\'' + ", logDirectory=" + logDirectory + '}';
	}
}
