package de.viadee.ki._SNAPSHOT;

import de.viadee.ki.sparkimporter.exceptions.FaultyConfigurationException;
import de.viadee.ki.sparkimporter.runner.CSVImportAndProcessingRunner;

/**
 * Hello world!
 *
 */
public class App 
{
	public static void main(String[] args){

		CSVImportAndProcessingRunner runner = new CSVImportAndProcessingRunner();
		
        try {
            runner.run(args);
        } catch (FaultyConfigurationException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
}
