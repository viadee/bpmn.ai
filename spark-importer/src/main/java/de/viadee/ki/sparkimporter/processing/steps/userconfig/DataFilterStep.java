package de.viadee.ki.sparkimporter.processing.steps.userconfig;

import de.viadee.ki.sparkimporter.configuration.Configuration;
import de.viadee.ki.sparkimporter.configuration.dataextraction.DataExtractionConfiguration;
import de.viadee.ki.sparkimporter.configuration.util.ConfigurationUtils;
import de.viadee.ki.sparkimporter.processing.interfaces.PreprocessingStepInterface;
import de.viadee.ki.sparkimporter.util.SparkImporterLogger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.Map;

public class DataFilterStep implements PreprocessingStepInterface {
    @Override
    public Dataset<Row> runPreprocessingStep(Dataset<Row> dataset, boolean writeStepResultIntoFile, String dataLevel, Map<String, Object> parameters) {

    	if (parameters == null || parameters.size() == 0) {
            SparkImporterLogger.getInstance().writeWarn("No parameters found for the DataFilterStep");
            return dataset;
        }
    	
    	String query = (String) parameters.get("query");               
        SparkImporterLogger.getInstance().writeInfo("Filtering data with filter query: " + query + ".");
        dataset = dataset.filter(query);

        if(dataset.count() == 0) {
            SparkImporterLogger.getInstance().writeInfo("Filtering resulted in zero lines of data. Aborting. Please check your filter query.");
            System.exit(1);
        }
               
        return dataset;
    }  
}
