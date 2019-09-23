package de.viadee.bpmnai.core;

import de.viadee.bpmnai.core.exceptions.FaultyConfigurationException;
import de.viadee.bpmnai.core.runner.SparkRunner;
import de.viadee.bpmnai.core.runner.impl.KafkaImportRunner;

public class KafkaImportApplication {

	public static void main(String[] arguments) {
		SparkRunner kafkaImportRunner = new KafkaImportRunner();
		try {
			kafkaImportRunner.run(arguments);
		} catch (FaultyConfigurationException e) {
			e.printStackTrace();
		}
	}

}
