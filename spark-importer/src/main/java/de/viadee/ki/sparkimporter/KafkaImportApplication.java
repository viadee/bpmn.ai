package de.viadee.ki.sparkimporter;

import de.viadee.ki.sparkimporter.exceptions.FaultyConfigurationException;
import de.viadee.ki.sparkimporter.runner.KafkaImportRunner;

public class KafkaImportApplication {


	public static void main(String[] arguments) {
		KafkaImportRunner kafkaImportRunner = new KafkaImportRunner();
		try {
			kafkaImportRunner.run(arguments);
		} catch (FaultyConfigurationException e) {
			e.printStackTrace();
		}
	}

}
