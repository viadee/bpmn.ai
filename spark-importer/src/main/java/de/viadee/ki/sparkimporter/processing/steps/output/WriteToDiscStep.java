package de.viadee.ki.sparkimporter.processing.steps.output;

import de.viadee.ki.sparkimporter.annotation.PreprocessingStepDescription;
import de.viadee.ki.sparkimporter.processing.interfaces.PreprocessingStepInterface;
import de.viadee.ki.sparkimporter.runner.SparkRunnerConfig;
import de.viadee.ki.sparkimporter.util.SparkImporterUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;

import java.util.Map;
import java.util.UUID;

@PreprocessingStepDescription(name = "Write to disc", description = "The resulting dataset is written into a file. It could e.g. also be written to a HDFS filesystem.")
public class WriteToDiscStep implements PreprocessingStepInterface {
    @Override
    public Dataset<Row> runPreprocessingStep(Dataset<Row> dataset, boolean writeStepResultIntoFile, String dataLevel, Map<String, Object> parameters, SparkRunnerConfig config) {
    	
        // remove spaces from column names as parquet does not support them
        for(String columnName : dataset.columns()) {
            if(columnName.contains(" ")) {
                String newColumnName = columnName.replace(' ', '_');
                dataset = dataset.withColumnRenamed(columnName, newColumnName);
            }
        }

        dataset = dataset.cache();

        SparkImporterUtils.getInstance().writeDatasetToParquet(dataset, "result", config);

        if(config.isGenerateJsonPreview()) {
            dataset.write().mode(SaveMode.Overwrite).saveAsTable("result");
            SparkImporterUtils.getInstance().writeDatasetToJson(dataset.limit(config.getJsonPreviewLineCount()), UUID.randomUUID().toString(), config);
        }

        return dataset;
    }
}
