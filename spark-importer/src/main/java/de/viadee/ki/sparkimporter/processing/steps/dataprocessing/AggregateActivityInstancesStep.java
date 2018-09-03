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

import static org.apache.spark.sql.functions.isnull;
import static org.apache.spark.sql.functions.not;

public class AggregateActivityInstancesStep implements PreprocessingStepInterface {

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
            } else {
                aggregationMap.put(column, "AllButEmptyString");
            }
        }

        //first aggregation
        //take only variableUpdate rows

        Dataset<Row> datasetVIAgg = null;

        datasetVIAgg = dataset
                .filter(isnull(dataset.col(SparkImporterVariables.VAR_STATE)))
                .groupBy(SparkImporterVariables.VAR_ACT_INST_ID, SparkImporterVariables.VAR_PROCESS_INSTANCE_VARIABLE_NAME)
                .agg(aggregationMap);

        //cleanup, so renaming columns and dropping not used ones
        datasetVIAgg = datasetVIAgg.drop(SparkImporterVariables.VAR_PROCESS_INSTANCE_ID);
        datasetVIAgg = datasetVIAgg.drop(SparkImporterVariables.VAR_ACT_INST_ID);
        datasetVIAgg = datasetVIAgg.drop(SparkImporterVariables.VAR_PROCESS_INSTANCE_VARIABLE_NAME);

        String pattern = "(first|max|allbutemptystring)\\((.+)\\)";
        Pattern r = Pattern.compile(pattern);

        for(String columnName : datasetVIAgg.columns()) {
            Matcher m = r.matcher(columnName);
            if(m.find()) {
                String newColumnName = m.group(2);
                datasetVIAgg = datasetVIAgg.withColumnRenamed(columnName, newColumnName);
            }
        }

        //union again with processInstance rows. we aggregate them as well to have the same columns
        dataset = dataset
                .select(
                        SparkImporterVariables.VAR_PROCESS_INSTANCE_ID,
                        SparkImporterVariables.VAR_STATE,
                        SparkImporterVariables.VAR_ACT_INST_ID,
                        SparkImporterVariables.VAR_ACT_TYPE,
                        SparkImporterVariables.VAR_ACT_NAME,
                        SparkImporterVariables.VAR_START_TIME,
                        SparkImporterVariables.VAR_PROCESS_INSTANCE_VARIABLE_NAME,
                        SparkImporterVariables.VAR_PROCESS_INSTANCE_VARIABLE_TYPE,
                        SparkImporterVariables.VAR_PROCESS_INSTANCE_VARIABLE_REVISION,
                        SparkImporterVariables.VAR_LONG,
                        SparkImporterVariables.VAR_DOUBLE,
                        SparkImporterVariables.VAR_TEXT,
                        SparkImporterVariables.VAR_TEXT2
                )
                .filter(not(isnull(dataset.col(SparkImporterVariables.VAR_STATE))))
                .union(datasetVIAgg
                        .select(
                                SparkImporterVariables.VAR_PROCESS_INSTANCE_ID,
                                SparkImporterVariables.VAR_STATE,
                                SparkImporterVariables.VAR_ACT_INST_ID,
                                SparkImporterVariables.VAR_ACT_TYPE,
                                SparkImporterVariables.VAR_ACT_NAME,
                                SparkImporterVariables.VAR_START_TIME,
                                SparkImporterVariables.VAR_PROCESS_INSTANCE_VARIABLE_NAME,
                                SparkImporterVariables.VAR_PROCESS_INSTANCE_VARIABLE_TYPE,
                                SparkImporterVariables.VAR_PROCESS_INSTANCE_VARIABLE_REVISION,
                                SparkImporterVariables.VAR_LONG,
                                SparkImporterVariables.VAR_DOUBLE,
                                SparkImporterVariables.VAR_TEXT,
                                SparkImporterVariables.VAR_TEXT2
                        ))
                .orderBy(SparkImporterVariables.VAR_ACT_INST_ID, SparkImporterVariables.VAR_START_TIME);

        if(writeStepResultIntoFile) {
            SparkImporterUtils.getInstance().writeDatasetToCSV(dataset, "agg_activity_instances");
        }

        //return preprocessed data
        return dataset;
    }
}
