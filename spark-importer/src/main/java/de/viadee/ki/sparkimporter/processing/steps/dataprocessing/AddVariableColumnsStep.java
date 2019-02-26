package de.viadee.ki.sparkimporter.processing.steps.dataprocessing;

import de.viadee.ki.sparkimporter.annotation.PreprocessingStepDescription;
import de.viadee.ki.sparkimporter.processing.interfaces.PreprocessingStepInterface;
import de.viadee.ki.sparkimporter.util.SparkBroadcastHelper;
import de.viadee.ki.sparkimporter.util.SparkImporterUtils;
import de.viadee.ki.sparkimporter.util.SparkImporterVariables;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.spark.sql.functions.*;

@PreprocessingStepDescription(value = "This is the first aggregation step. In this step all variable updates are aggregated per process instance and variable. So if one variable value changed during a process instance it is aggregated to the last value the variable had in the process instance.")
public class AddVariableColumnsStep implements PreprocessingStepInterface {

    @Override
    public Dataset<Row> runPreprocessingStep(Dataset<Row> dataset, boolean writeStepResultIntoFile, String dataLevel, Map<String, Object> parameters) {

        // AGGREGATE VARIABLE UPDATES
        dataset = doVariableUpdatesAggregation(dataset, writeStepResultIntoFile, dataLevel);

        // ADD VARIABLE COLUMNS
        dataset = doAddVariableColumns(dataset, writeStepResultIntoFile, dataLevel);

        return dataset;
    }

    private Dataset<Row> doVariableUpdatesAggregation(Dataset<Row> dataset, boolean writeStepResultIntoFile, String dataLevel) {
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

        Dataset<Row> datasetVUAgg = null;


        if(dataLevel.equals(SparkImporterVariables.DATA_LEVEL_PROCESS)) {
            if (Arrays.asList(dataset.columns()).contains(SparkImporterVariables.VAR_TIMESTAMP)) {
                datasetVUAgg = dataset
                        .filter(isnull(dataset.col(SparkImporterVariables.VAR_STATE)))
                        .orderBy(desc(SparkImporterVariables.VAR_TIMESTAMP))
                        .groupBy(SparkImporterVariables.VAR_PROCESS_INSTANCE_ID, SparkImporterVariables.VAR_PROCESS_INSTANCE_VARIABLE_NAME)
                        .agg(aggregationMap);
            } else {
                datasetVUAgg = dataset
                        .filter(isnull(dataset.col(SparkImporterVariables.VAR_STATE)))
                        .groupBy(SparkImporterVariables.VAR_PROCESS_INSTANCE_ID, SparkImporterVariables.VAR_PROCESS_INSTANCE_VARIABLE_NAME)
                        .agg(aggregationMap);
            }

        } else {
            datasetVUAgg = dataset
                    .filter(isnull(dataset.col(SparkImporterVariables.VAR_STATE)))
                    .groupBy(SparkImporterVariables.VAR_ACT_INST_ID, SparkImporterVariables.VAR_PROCESS_INSTANCE_VARIABLE_NAME)
                    .agg(aggregationMap);
        }
        //cleanup, so renaming columns and dropping not used ones
        datasetVUAgg = datasetVUAgg.drop(SparkImporterVariables.VAR_PROCESS_INSTANCE_ID);
        datasetVUAgg = datasetVUAgg.drop(SparkImporterVariables.VAR_PROCESS_INSTANCE_VARIABLE_NAME);

        if(dataLevel.equals(SparkImporterVariables.DATA_LEVEL_ACTIVITY)) {
            datasetVUAgg = datasetVUAgg.drop(SparkImporterVariables.VAR_ACT_INST_ID);
        }

        String pattern = "(first|max|allbutemptystring)\\((.+)\\)";
        Pattern r = Pattern.compile(pattern);

        for(String columnName : datasetVUAgg.columns()) {
            Matcher m = r.matcher(columnName);
            if(m.find()) {
                String newColumnName = m.group(2);
                datasetVUAgg = datasetVUAgg.withColumnRenamed(columnName, newColumnName);
            }
        }

        if(dataLevel.equals(SparkImporterVariables.DATA_LEVEL_PROCESS)) {
            //union again with processInstance rows. we aggregate them as well to have the same columns
            dataset = dataset
                    .select(
                            SparkImporterVariables.VAR_PROCESS_INSTANCE_ID,
                            SparkImporterVariables.VAR_STATE,
                            SparkImporterVariables.VAR_PROCESS_INSTANCE_VARIABLE_NAME,
                            SparkImporterVariables.VAR_PROCESS_INSTANCE_VARIABLE_TYPE,
                            SparkImporterVariables.VAR_PROCESS_INSTANCE_VARIABLE_REVISION,
                            SparkImporterVariables.VAR_LONG,
                            SparkImporterVariables.VAR_DOUBLE,
                            SparkImporterVariables.VAR_TEXT,
                            SparkImporterVariables.VAR_TEXT2
                    )
                    .filter(not(isnull(dataset.col(SparkImporterVariables.VAR_STATE))))
                    .union(datasetVUAgg
                            .select(
                                    SparkImporterVariables.VAR_PROCESS_INSTANCE_ID,
                                    SparkImporterVariables.VAR_STATE,
                                    SparkImporterVariables.VAR_PROCESS_INSTANCE_VARIABLE_NAME,
                                    SparkImporterVariables.VAR_PROCESS_INSTANCE_VARIABLE_TYPE,
                                    SparkImporterVariables.VAR_PROCESS_INSTANCE_VARIABLE_REVISION,
                                    SparkImporterVariables.VAR_LONG,
                                    SparkImporterVariables.VAR_DOUBLE,
                                    SparkImporterVariables.VAR_TEXT,
                                    SparkImporterVariables.VAR_TEXT2
                            ));
        } else {
            //union again with processInstance rows. we aggregate them as well to have the same columns
            dataset = dataset
                    .select(
                            SparkImporterVariables.VAR_PROCESS_INSTANCE_ID,
                            SparkImporterVariables.VAR_STATE,
                            SparkImporterVariables.VAR_ACT_INST_ID,
                            SparkImporterVariables.VAR_START_TIME,
                            SparkImporterVariables.VAR_END_TIME,
                            SparkImporterVariables.VAR_DURATION,
                            SparkImporterVariables.VAR_PROCESS_INSTANCE_VARIABLE_NAME,
                            SparkImporterVariables.VAR_PROCESS_INSTANCE_VARIABLE_TYPE,
                            SparkImporterVariables.VAR_PROCESS_INSTANCE_VARIABLE_REVISION,
                            SparkImporterVariables.VAR_LONG,
                            SparkImporterVariables.VAR_DOUBLE,
                            SparkImporterVariables.VAR_TEXT,
                            SparkImporterVariables.VAR_TEXT2
                    )
                    .filter(not(isnull(dataset.col(SparkImporterVariables.VAR_STATE))))
                    .union(datasetVUAgg
                            .select(
                                    SparkImporterVariables.VAR_PROCESS_INSTANCE_ID,
                                    SparkImporterVariables.VAR_STATE,
                                    SparkImporterVariables.VAR_ACT_INST_ID,
                                    SparkImporterVariables.VAR_START_TIME,
                                    SparkImporterVariables.VAR_END_TIME,
                                    SparkImporterVariables.VAR_DURATION,
                                    SparkImporterVariables.VAR_PROCESS_INSTANCE_VARIABLE_NAME,
                                    SparkImporterVariables.VAR_PROCESS_INSTANCE_VARIABLE_TYPE,
                                    SparkImporterVariables.VAR_PROCESS_INSTANCE_VARIABLE_REVISION,
                                    SparkImporterVariables.VAR_LONG,
                                    SparkImporterVariables.VAR_DOUBLE,
                                    SparkImporterVariables.VAR_TEXT,
                                    SparkImporterVariables.VAR_TEXT2
                            ))
                    .orderBy(SparkImporterVariables.VAR_ACT_INST_ID, SparkImporterVariables.VAR_START_TIME);
        }

        if(writeStepResultIntoFile) {
            SparkImporterUtils.getInstance().writeDatasetToCSV(dataset, "agg_variable_updates");
        }

        //return preprocessed data
        return dataset;
    }

    private Dataset<Row> doAddVariableColumns(Dataset<Row> dataset, boolean writeStepResultIntoFile, String dataLevel) {
        Map<String, String> varMap = (Map<String, String>) SparkBroadcastHelper.getInstance().getBroadcastVariable(SparkBroadcastHelper.BROADCAST_VARIABLE.PROCESS_VARIABLES_ESCALATED);
        Set<String> variables = varMap.keySet();

        for(String v : variables) {
            dataset = dataset.withColumn(v, when(dataset.col(SparkImporterVariables.VAR_PROCESS_INSTANCE_VARIABLE_NAME).equalTo(v),
                    when(dataset.col(SparkImporterVariables.VAR_PROCESS_INSTANCE_VARIABLE_TYPE).equalTo("string"), dataset.col(SparkImporterVariables.VAR_TEXT))
                            .when(dataset.col(SparkImporterVariables.VAR_PROCESS_INSTANCE_VARIABLE_TYPE).equalTo("null"), dataset.col(SparkImporterVariables.VAR_TEXT))
                            .when(dataset.col(SparkImporterVariables.VAR_PROCESS_INSTANCE_VARIABLE_TYPE).equalTo("boolean"), dataset.col(SparkImporterVariables.VAR_LONG))
                            .when(dataset.col(SparkImporterVariables.VAR_PROCESS_INSTANCE_VARIABLE_TYPE).equalTo("integer"), dataset.col(SparkImporterVariables.VAR_LONG))
                            .when(dataset.col(SparkImporterVariables.VAR_PROCESS_INSTANCE_VARIABLE_TYPE).equalTo("long"), dataset.col(SparkImporterVariables.VAR_LONG))
                            .when(dataset.col(SparkImporterVariables.VAR_PROCESS_INSTANCE_VARIABLE_TYPE).equalTo("double"), dataset.col(SparkImporterVariables.VAR_DOUBLE))
                            .when(dataset.col(SparkImporterVariables.VAR_PROCESS_INSTANCE_VARIABLE_TYPE).equalTo("date"), dataset.col(SparkImporterVariables.VAR_LONG))
                            .otherwise(dataset.col(SparkImporterVariables.VAR_TEXT2)))
                    .otherwise(null));

            //rev count is only relevant on process level
            if(dataLevel.equals(SparkImporterVariables.DATA_LEVEL_PROCESS) && SparkImporterVariables.isRevCountEnabled()) {
                dataset = dataset.withColumn(v+"_rev",
                        when(dataset.col(SparkImporterVariables.VAR_PROCESS_INSTANCE_VARIABLE_NAME).equalTo(v), dataset.col(SparkImporterVariables.VAR_PROCESS_INSTANCE_VARIABLE_REVISION))
                                .otherwise("0"));
            }
        }

        //drop unnecesssary columns
        dataset = dataset.drop(
                SparkImporterVariables.VAR_PROCESS_INSTANCE_VARIABLE_TYPE,
                SparkImporterVariables.VAR_PROCESS_INSTANCE_VARIABLE_REVISION,
                SparkImporterVariables.VAR_DOUBLE,
                SparkImporterVariables.VAR_LONG,
                SparkImporterVariables.VAR_TEXT,
                SparkImporterVariables.VAR_TEXT2);

        if(!SparkImporterVariables.isDevProcessStateColumnWorkaroundEnabled()) {
            dataset = dataset.drop(SparkImporterVariables.VAR_PROCESS_INSTANCE_VARIABLE_NAME);
        }

        if(writeStepResultIntoFile) {
            SparkImporterUtils.getInstance().writeDatasetToCSV(dataset, "add_var_columns");
        }

        //return preprocessed data
        return dataset;
    }
}
