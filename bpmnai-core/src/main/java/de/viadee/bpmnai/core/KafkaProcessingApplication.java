package de.viadee.bpmnai.core;

import de.viadee.bpmnai.core.exceptions.FaultyConfigurationException;
import de.viadee.bpmnai.core.runner.SparkRunner;
import de.viadee.bpmnai.core.runner.impl.KafkaProcessingRunner;

public class KafkaProcessingApplication {

	public static void main(String[] arguments) {
		SparkRunner kafkaProcessingRunner = new KafkaProcessingRunner();
		try {
			kafkaProcessingRunner.run(arguments);
		} catch (FaultyConfigurationException e) {
			e.printStackTrace();
		}
	}

}
