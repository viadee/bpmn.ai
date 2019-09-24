package de.viadee.bpmnai.core.processing.steps.output;

import de.viadee.bpmnai.core.util.BpmnaiUtils;
import de.viadee.bpmnai.core.annotation.PreprocessingStepDescription;
import de.viadee.bpmnai.core.processing.interfaces.PreprocessingStepInterface;
import de.viadee.bpmnai.core.runner.config.SparkRunnerConfig;
import de.viadee.bpmnai.core.util.BpmnaiVariables;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;

import java.util.Map;

@PreprocessingStepDescription(name = "Write to disc", description = "The resulting dataset is written into a file. It could e.g. also be written to a HDFS filesystem.")
public class WriteToDiscStep implements PreprocessingStepInterface {
    @Override
    public Dataset<Row> runPreprocessingStep(Dataset<Row> dataset, Map<String, Object> parameters, SparkRunnerConfig config) {
    	
        // remove spaces from column names as parquet does not support them
        for(String columnName : dataset.columns()) {
            if(columnName.contains(" ")) {
                String newColumnName = columnName.replace(' ', '_');
                dataset = dataset.withColumnRenamed(columnName, newColumnName);
            }
        }

        dataset.cache();
        BpmnaiUtils.getInstance().writeDatasetToParquet(dataset, "result", config);

        if(config.isGenerateResultPreview()) {
            dataset.limit(config.getResultPreviewLineCount()).write().mode(SaveMode.Overwrite).saveAsTable(BpmnaiVariables.RESULT_PREVIEW_TEMP_TABLE);
        }

        return dataset;
    }
}
