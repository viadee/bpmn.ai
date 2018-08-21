package de.viadee.ki.sparkimporter.processing.steps.dataprocessing;

import de.viadee.ki.sparkimporter.processing.interfaces.PreprocessingStepInterface;
import de.viadee.ki.sparkimporter.util.SparkImporterUtils;
import de.viadee.ki.sparkimporter.util.SparkImporterVariables;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.spark.sql.functions.*;

public class AggregateVariableUpdatesStep implements PreprocessingStepInterface {

    @Override
    public Dataset<Row> runPreprocessingStep(Dataset<Row> dataset, boolean writeStepResultIntoFile) {

        //apply max aggregator to known date columns start_time_ and end_time_ so that no date formatting is done in custom aggregator
        List<String> dateFormatColumns = Arrays.asList(new String[]{SparkImporterVariables.VAR_START_TIME, SparkImporterVariables.VAR_END_TIME});

        Map<String, String> aggregationMap = new HashMap<>();
        for(String column : dataset.columns()) {
            if(column.endsWith("_rev")) {
                aggregationMap.put(column, "max");
            } else if(dateFormatColumns.contains(column)) {
                aggregationMap.put(column, "first");
            } else if(column.equals(SparkImporterVariables.VAR_STATE)) {
                aggregationMap.put(column, "ProcessState");
            } else {
                aggregationMap.put(column, "AllButEmptyString");
            }
        }

        //first aggregation
        //take only variableUpdate rows

        Dataset<Row> datasetVUAgg = null;

        if(Arrays.asList(dataset.columns()).contains("timestamp_")) {
            datasetVUAgg = dataset
                    .filter(isnull(dataset.col(SparkImporterVariables.VAR_STATE)))
                    .orderBy(desc("timestamp_"))
                    .groupBy(SparkImporterVariables.VAR_PROCESS_INSTANCE_ID, SparkImporterVariables.VAR_PROCESS_INSTANCE_VARIABLE_NAME)
                    .agg(aggregationMap);
        } else {
            datasetVUAgg = dataset
                    .filter(isnull(dataset.col(SparkImporterVariables.VAR_STATE)))
                    .groupBy(SparkImporterVariables.VAR_PROCESS_INSTANCE_ID, SparkImporterVariables.VAR_PROCESS_INSTANCE_VARIABLE_NAME)
                    .agg(aggregationMap);
        }

        //cleanup, so renaming columns and dropping not used ones
        datasetVUAgg = datasetVUAgg.drop(SparkImporterVariables.VAR_PROCESS_INSTANCE_ID);
        datasetVUAgg = datasetVUAgg.drop(SparkImporterVariables.VAR_PROCESS_INSTANCE_VARIABLE_NAME);

        //union again with processInstance rows
        dataset = dataset
                .filter(not(isnull(dataset.col(SparkImporterVariables.VAR_STATE))))
                .union(datasetVUAgg);


        String pattern = "(first|max|allbutemptystring|processstate)\\((.+)\\)";
        Pattern r = Pattern.compile(pattern);

        for(String columnName : dataset.columns()) {
            Matcher m = r.matcher(columnName);
            if(m.find()) {
                String newColumnName = m.group(2);
                dataset = dataset.withColumnRenamed(columnName, newColumnName);
            }
        }

        if(writeStepResultIntoFile) {
            SparkImporterUtils.getInstance().writeDatasetToCSV(dataset, "agg_to_process_instance");
        }

        //return preprocessed data
        return dataset;
    }
}
