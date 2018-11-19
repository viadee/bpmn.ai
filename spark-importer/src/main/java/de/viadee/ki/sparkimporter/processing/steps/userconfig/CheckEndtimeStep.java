package de.viadee.ki.sparkimporter.processing.steps.userconfig;

import de.viadee.ki.sparkimporter.configuration.Configuration;
import de.viadee.ki.sparkimporter.configuration.preprocessing.PreprocessingConfiguration;
import de.viadee.ki.sparkimporter.configuration.preprocessing.VariableConfiguration;
import de.viadee.ki.sparkimporter.configuration.util.ConfigurationUtils;
import de.viadee.ki.sparkimporter.processing.interfaces.PreprocessingStepInterface;
import de.viadee.ki.sparkimporter.util.SparkImporterLogger;
import de.viadee.ki.sparkimporter.util.SparkImporterUtils;
import de.viadee.ki.sparkimporter.util.SparkImporterVariables;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class CheckEndtimeStep implements PreprocessingStepInterface {
    @Override
    	 public Dataset<Row> runPreprocessingStep(Dataset<Row> dataset, boolean writeStepResultIntoFile, String dataLevel,
                 Map<String, Object> parameters) {

			if (parameters == null) {
			SparkImporterLogger.getInstance().writeWarn("No parameters found for the CheckEndtimeStep");
			return dataset;
			}
			
			final SparkSession sparkSession = SparkSession.builder().getOrCreate();
			String colName = (String) parameters.get("column");
			
		
			dataset = dataset.withColumn("finished", functions.when(dataset.col(colName).isNull(), "false").
					otherwise("true"));
			

			return dataset;
			}

        
}
