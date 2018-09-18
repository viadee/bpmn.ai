package de.viadee.ki.sparkimporter;

import de.viadee.ki.sparkimporter.exceptions.FaultyConfigurationException;
import de.viadee.ki.sparkimporter.runner.KafkaProcessingRunner;
import de.viadee.ki.sparkimporter.util.SparkImporterKafkaDataProcessingArguments;

public class KafkaProcessingApplication {

	public static SparkImporterKafkaDataProcessingArguments ARGS;

	public static void main(String[] arguments) {
		KafkaProcessingRunner kafkaProcessingRunner = new KafkaProcessingRunner();
		try {
			kafkaProcessingRunner.run(arguments);
		} catch (FaultyConfigurationException e) {
			e.printStackTrace();
		}
	}

}
