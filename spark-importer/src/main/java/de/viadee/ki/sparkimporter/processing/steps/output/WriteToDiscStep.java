package de.viadee.ki.sparkimporter.processing.steps.output;

import de.viadee.ki.sparkimporter.processing.interfaces.PreprocessingStepInterface;
import de.viadee.ki.sparkimporter.util.SparkImporterUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.Map;

public class WriteToDiscStep implements PreprocessingStepInterface {
    @Override
    public Dataset<Row> runPreprocessingStep(Dataset<Row> dataset, boolean writeStepResultIntoFile, String dataLevel, Map<String, Object> parameters) {
    	
        // remove spaces from column names as parquet does not support them
        for(String columnName : dataset.columns()) {
            if(columnName.contains(" ")) {
                String newColumnName = columnName.replace(' ', '_');
                dataset = dataset.withColumnRenamed(columnName, newColumnName);
            }
        }

        SparkImporterUtils.getInstance().writeDatasetToParquet(dataset, "result");

        return dataset;
    }
}
