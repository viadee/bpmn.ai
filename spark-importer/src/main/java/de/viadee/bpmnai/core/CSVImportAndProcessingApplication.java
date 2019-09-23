package de.viadee.bpmnai.core;

import de.viadee.bpmnai.core.exceptions.FaultyConfigurationException;
import de.viadee.bpmnai.core.runner.impl.CSVImportAndProcessingRunner;
import de.viadee.bpmnai.core.runner.SparkRunner;

public class CSVImportAndProcessingApplication {

	public static void main(String[] arguments) {
		SparkRunner csvImportAndProcessingRunner = new CSVImportAndProcessingRunner();
		try {
			csvImportAndProcessingRunner.run(arguments);
		} catch (FaultyConfigurationException e) {
			e.printStackTrace();
		}
	}

}
