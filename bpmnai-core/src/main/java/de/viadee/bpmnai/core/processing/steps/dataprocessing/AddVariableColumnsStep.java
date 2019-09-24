package de.viadee.bpmnai.core.processing.steps.dataprocessing;

import de.viadee.bpmnai.core.util.BpmnaiUtils;
import de.viadee.bpmnai.core.annotation.PreprocessingStepDescription;
import de.viadee.bpmnai.core.processing.interfaces.PreprocessingStepInterface;
import de.viadee.bpmnai.core.runner.config.SparkRunnerConfig;
import de.viadee.bpmnai.core.util.BpmnaiVariables;
import de.viadee.bpmnai.core.util.helper.SparkBroadcastHelper;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.spark.sql.functions.*;

@PreprocessingStepDescription(name = "Add variable columns", description = "In this step all process variables detected in prior steps are added as separate columns to the dataset.")
public class AddVariableColumnsStep implements PreprocessingStepInterface {

    @Override
    public Dataset<Row> runPreprocessingStep(Dataset<Row> dataset, Map<String, Object> parameters, SparkRunnerConfig config) {

        // AGGREGATE VARIABLE UPDATES
        dataset = doVariableUpdatesAggregation(dataset, config.isWriteStepResultsIntoFile(), config.getDataLevel(), config);

        // ADD VARIABLE COLUMNS
        dataset = doAddVariableColumns(dataset, config.isWriteStepResultsIntoFile(), config.getDataLevel(), config);

        return dataset;
    }

    private Dataset<Row> doVariableUpdatesAggregation(Dataset<Row> dataset, boolean writeStepResultIntoFile, String dataLevel, SparkRunnerConfig config) {
        //apply max aggregator to known date columns start_time_ and end_time_ so that no date formatting is done in custom aggregator
        List<String> dateFormatColumns = Arrays.asList(new String[]{BpmnaiVariables.VAR_START_TIME, BpmnaiVariables.VAR_END_TIME});

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

        Dataset<Row> datasetVUAgg = dataset;
        datasetVUAgg = datasetVUAgg.filter(dataset.col(BpmnaiVariables.VAR_DATA_SOURCE).equalTo(BpmnaiVariables.EVENT_VARIABLE_UPDATE));

        if(dataLevel.equals(BpmnaiVariables.DATA_LEVEL_PROCESS)) {
            if (Arrays.asList(dataset.columns()).contains(BpmnaiVariables.VAR_TIMESTAMP)) {
                datasetVUAgg = datasetVUAgg
                        .orderBy(desc(BpmnaiVariables.VAR_TIMESTAMP));
            }

            datasetVUAgg = datasetVUAgg
                    .groupBy(BpmnaiVariables.VAR_PROCESS_INSTANCE_ID, BpmnaiVariables.VAR_PROCESS_INSTANCE_VARIABLE_NAME)
                    .agg(aggregationMap);

        } else {
            datasetVUAgg = datasetVUAgg
                    .groupBy(BpmnaiVariables.VAR_ACT_INST_ID, BpmnaiVariables.VAR_PROCESS_INSTANCE_VARIABLE_NAME)
                    .agg(aggregationMap);
        }

        //cleanup, so renaming columns and dropping not used ones
        datasetVUAgg = datasetVUAgg.drop(BpmnaiVariables.VAR_PROCESS_INSTANCE_ID);
        datasetVUAgg = datasetVUAgg.drop(BpmnaiVariables.VAR_PROCESS_INSTANCE_VARIABLE_NAME);

        if(dataLevel.equals(BpmnaiVariables.DATA_LEVEL_ACTIVITY)) {
            datasetVUAgg = datasetVUAgg.drop(BpmnaiVariables.VAR_ACT_INST_ID);
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

        if(dataLevel.equals(BpmnaiVariables.DATA_LEVEL_PROCESS)) {
            //union again with processInstance rows. we aggregate them as well to have the same columns
            dataset = dataset
                    .select(
                            BpmnaiVariables.VAR_PROCESS_INSTANCE_ID,
                            BpmnaiVariables.VAR_STATE,
                            BpmnaiVariables.VAR_PROCESS_INSTANCE_VARIABLE_NAME,
                            BpmnaiVariables.VAR_PROCESS_INSTANCE_VARIABLE_TYPE,
                            BpmnaiVariables.VAR_PROCESS_INSTANCE_VARIABLE_REVISION,
                            BpmnaiVariables.VAR_LONG,
                            BpmnaiVariables.VAR_DOUBLE,
                            BpmnaiVariables.VAR_TEXT,
                            BpmnaiVariables.VAR_TEXT2,
                            BpmnaiVariables.VAR_DATA_SOURCE
                    )
                    .filter(dataset.col(BpmnaiVariables.VAR_DATA_SOURCE).equalTo(BpmnaiVariables.EVENT_PROCESS_INSTANCE))
                    .union(datasetVUAgg
                            .select(
                                    BpmnaiVariables.VAR_PROCESS_INSTANCE_ID,
                                    BpmnaiVariables.VAR_STATE,
                                    BpmnaiVariables.VAR_PROCESS_INSTANCE_VARIABLE_NAME,
                                    BpmnaiVariables.VAR_PROCESS_INSTANCE_VARIABLE_TYPE,
                                    BpmnaiVariables.VAR_PROCESS_INSTANCE_VARIABLE_REVISION,
                                    BpmnaiVariables.VAR_LONG,
                                    BpmnaiVariables.VAR_DOUBLE,
                                    BpmnaiVariables.VAR_TEXT,
                                    BpmnaiVariables.VAR_TEXT2,
                                    BpmnaiVariables.VAR_DATA_SOURCE
                            ));
        } else {
            //union again with processInstance rows. we aggregate them as well to have the same columns
            dataset = dataset
                    .select(
                            BpmnaiVariables.VAR_PROCESS_INSTANCE_ID,
                            BpmnaiVariables.VAR_STATE,
                            BpmnaiVariables.VAR_ACT_INST_ID,
                            BpmnaiVariables.VAR_START_TIME,
                            BpmnaiVariables.VAR_END_TIME,
                            BpmnaiVariables.VAR_DURATION,
                            BpmnaiVariables.VAR_PROCESS_INSTANCE_VARIABLE_NAME,
                            BpmnaiVariables.VAR_PROCESS_INSTANCE_VARIABLE_TYPE,
                            BpmnaiVariables.VAR_PROCESS_INSTANCE_VARIABLE_REVISION,
                            BpmnaiVariables.VAR_LONG,
                            BpmnaiVariables.VAR_DOUBLE,
                            BpmnaiVariables.VAR_TEXT,
                            BpmnaiVariables.VAR_TEXT2,
                            BpmnaiVariables.VAR_DATA_SOURCE
                    )
                    .filter(not(isnull(dataset.col(BpmnaiVariables.VAR_START_TIME))))
                    .union(datasetVUAgg
                            .select(
                                    BpmnaiVariables.VAR_PROCESS_INSTANCE_ID,
                                    BpmnaiVariables.VAR_STATE,
                                    BpmnaiVariables.VAR_ACT_INST_ID,
                                    BpmnaiVariables.VAR_START_TIME,
                                    BpmnaiVariables.VAR_END_TIME,
                                    BpmnaiVariables.VAR_DURATION,
                                    BpmnaiVariables.VAR_PROCESS_INSTANCE_VARIABLE_NAME,
                                    BpmnaiVariables.VAR_PROCESS_INSTANCE_VARIABLE_TYPE,
                                    BpmnaiVariables.VAR_PROCESS_INSTANCE_VARIABLE_REVISION,
                                    BpmnaiVariables.VAR_LONG,
                                    BpmnaiVariables.VAR_DOUBLE,
                                    BpmnaiVariables.VAR_TEXT,
                                    BpmnaiVariables.VAR_TEXT2,
                                    BpmnaiVariables.VAR_DATA_SOURCE
                            ))
                    .orderBy(BpmnaiVariables.VAR_ACT_INST_ID, BpmnaiVariables.VAR_START_TIME);
        }

        if(writeStepResultIntoFile) {
            BpmnaiUtils.getInstance().writeDatasetToCSV(dataset, "agg_variable_updates", config);
        }

        //return preprocessed data
        return dataset;
    }

    private Dataset<Row> doAddVariableColumns(Dataset<Row> dataset, boolean writeStepResultIntoFile, String dataLevel, SparkRunnerConfig config) {
        Map<String, String> varMap = (Map<String, String>) SparkBroadcastHelper.getInstance().getBroadcastVariable(SparkBroadcastHelper.BROADCAST_VARIABLE.PROCESS_VARIABLES_ESCALATED);
        Set<String> variables = varMap.keySet();

        for(String v : variables) {
            dataset = dataset.withColumn(v, when(dataset.col(BpmnaiVariables.VAR_PROCESS_INSTANCE_VARIABLE_NAME).equalTo(v),
                    when(dataset.col(BpmnaiVariables.VAR_PROCESS_INSTANCE_VARIABLE_TYPE).equalTo("string"), dataset.col(BpmnaiVariables.VAR_TEXT))
                            .when(dataset.col(BpmnaiVariables.VAR_PROCESS_INSTANCE_VARIABLE_TYPE).equalTo("null"), dataset.col(BpmnaiVariables.VAR_TEXT))
                            .when(dataset.col(BpmnaiVariables.VAR_PROCESS_INSTANCE_VARIABLE_TYPE).equalTo("boolean"), dataset.col(BpmnaiVariables.VAR_LONG))
                            .when(dataset.col(BpmnaiVariables.VAR_PROCESS_INSTANCE_VARIABLE_TYPE).equalTo("integer"), dataset.col(BpmnaiVariables.VAR_LONG))
                            .when(dataset.col(BpmnaiVariables.VAR_PROCESS_INSTANCE_VARIABLE_TYPE).equalTo("long"), dataset.col(BpmnaiVariables.VAR_LONG))
                            .when(dataset.col(BpmnaiVariables.VAR_PROCESS_INSTANCE_VARIABLE_TYPE).equalTo("double"), dataset.col(BpmnaiVariables.VAR_DOUBLE))
                            .when(dataset.col(BpmnaiVariables.VAR_PROCESS_INSTANCE_VARIABLE_TYPE).equalTo("date"), dataset.col(BpmnaiVariables.VAR_LONG))
                            .otherwise(dataset.col(BpmnaiVariables.VAR_TEXT2)))
                    .otherwise(null));

            //rev count is only relevant on process level
            if(dataLevel.equals(BpmnaiVariables.DATA_LEVEL_PROCESS) && config.isRevCountEnabled()) {
                dataset = dataset.withColumn(v+"_rev",
                        when(dataset.col(BpmnaiVariables.VAR_PROCESS_INSTANCE_VARIABLE_NAME).equalTo(v), dataset.col(BpmnaiVariables.VAR_PROCESS_INSTANCE_VARIABLE_REVISION))
                                .otherwise("0"));
            }
        }

        //drop unnecesssary columns
        dataset = dataset.drop(
                BpmnaiVariables.VAR_PROCESS_INSTANCE_VARIABLE_TYPE,
                BpmnaiVariables.VAR_PROCESS_INSTANCE_VARIABLE_REVISION,
                BpmnaiVariables.VAR_DOUBLE,
                BpmnaiVariables.VAR_LONG,
                BpmnaiVariables.VAR_TEXT,
                BpmnaiVariables.VAR_TEXT2);

        if(!config.isDevProcessStateColumnWorkaroundEnabled()) {
            dataset = dataset.drop(BpmnaiVariables.VAR_PROCESS_INSTANCE_VARIABLE_NAME);
        }

        if(writeStepResultIntoFile) {
            BpmnaiUtils.getInstance().writeDatasetToCSV(dataset, "add_var_columns", config);
        }

        //return preprocessed data
        return dataset;
    }
}
