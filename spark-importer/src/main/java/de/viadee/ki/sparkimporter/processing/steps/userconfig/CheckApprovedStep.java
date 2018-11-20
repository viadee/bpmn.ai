package de.viadee.ki.sparkimporter.processing.steps.userconfig;

import de.viadee.ki.sparkimporter.processing.interfaces.PreprocessingStepInterface;
import de.viadee.ki.sparkimporter.util.SparkImporterLogger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;

import java.util.Map;

public class CheckApprovedStep implements PreprocessingStepInterface {
    @Override
    	 public Dataset<Row> runPreprocessingStep(Dataset<Row> dataset, boolean writeStepResultIntoFile, String dataLevel,
                 Map<String, Object> parameters) {

			if (parameters == null) {
			SparkImporterLogger.getInstance().writeWarn("No parameters found for the CheckEndtimeStep");
			return dataset;
			}			
	
			String colName = (String) parameters.get("column");
					
			dataset =  dataset.withColumn("approved2", functions.when(dataset.col(colName).equalTo("true"), "OK").otherwise("NOT OK"));
						
			return dataset;
			}

        
}
