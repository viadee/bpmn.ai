package de.viadee.ki.sparkimporter;

import de.viadee.ki.sparkimporter.exceptions.FaultyConfigurationException;
import de.viadee.ki.sparkimporter.runner.SparkRunner;
import de.viadee.ki.sparkimporter.runner.impl.CSVImportAndProcessingRunner;

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
