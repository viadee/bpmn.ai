package de.viadee.bpmnai.core.processing.steps.dataprocessing;

import de.viadee.bpmnai.core.util.BpmnaiUtils;
import de.viadee.bpmnai.core.annotation.PreprocessingStepDescription;
import de.viadee.bpmnai.core.processing.PreprocessingRunner;
import de.viadee.bpmnai.core.processing.interfaces.PreprocessingStepInterface;
import de.viadee.bpmnai.core.runner.config.SparkRunnerConfig;
import de.viadee.bpmnai.core.util.BpmnaiVariables;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import scala.collection.Seq;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@PreprocessingStepDescription(name = "Add reduced columns", description = "In the beginning the non relevant columns where removed to speed up the processing. These columns are now added back to the dataset by using the processInstanceId as a reference.")
public class AddReducedColumnsToDatasetStep implements PreprocessingStepInterface {

    @Override
    public Dataset<Row> runPreprocessingStep(Dataset<Row> dataset, Map<String, Object> parameters, SparkRunnerConfig config) {


        // take columns available initially from helper dataset and select the ones to be added back again
        List<String> existingColumns = Arrays.asList(dataset.columns());
        Dataset<Row> startColumns = PreprocessingRunner.helper_datasets.get("startColumns" + "_" + config.getDataLevel());
        List<String> columnNamesString = new ArrayList<>();
        List<Column> columnNames = new ArrayList<>();
        List<String> columnsNotBeAddedAgain = Arrays.asList(new String[]{
                BpmnaiVariables.VAR_PROCESS_INSTANCE_VARIABLE_TYPE,
                BpmnaiVariables.VAR_PROCESS_INSTANCE_VARIABLE_REVISION,
                BpmnaiVariables.VAR_LONG,
                BpmnaiVariables.VAR_DOUBLE,
                BpmnaiVariables.VAR_TEXT,
                BpmnaiVariables.VAR_TEXT2,
                BpmnaiVariables.VAR_TIMESTAMP,
                BpmnaiVariables.VAR_SEQUENCE_COUNTER,
                BpmnaiVariables.VAR_PROCESS_INSTANCE_VARIABLE_INSTANCE_ID,
                BpmnaiVariables.VAR_DATA_SOURCE
        });

        if(!config.isDevProcessStateColumnWorkaroundEnabled()) {
            columnsNotBeAddedAgain = Stream.concat(columnsNotBeAddedAgain.stream(), Stream.of(BpmnaiVariables.VAR_PROCESS_INSTANCE_VARIABLE_NAME)).collect(Collectors.toList());
        }

        columnNames.add(new Column(BpmnaiVariables.VAR_PROCESS_INSTANCE_ID));
        columnNames.add(new Column(BpmnaiVariables.VAR_STATE));
        columnNames.add(new Column(BpmnaiVariables.VAR_ACT_INST_ID));
        for(Row row : startColumns.collectAsList()) {
            String column = row.getString(0);
            if(!existingColumns.contains(column) && !columnsNotBeAddedAgain.contains(column)) {
                columnNamesString.add(column);
                columnNames.add(new Column(column));
            }
        }
        Seq<Column> selectionColumns = BpmnaiUtils.getInstance().asSeq(columnNames);

        //get relevant data from initial dataset to be added back again
        Dataset<Row> initialDataset = PreprocessingRunner.helper_datasets.get(PreprocessingRunner.DATASET_INITIAL + "_" + config.getDataLevel());
        Map<String, String> aggregationMap = new HashMap<>();
        for(String column : columnNamesString) {
            aggregationMap.put(column, "first");
        }

        Column filter = initialDataset.col(BpmnaiVariables.VAR_STATE).isNotNull();
        if(config.isDevProcessStateColumnWorkaroundEnabled() && config.getDataLevel().equals(BpmnaiVariables.DATA_LEVEL_PROCESS)) {
            filter = initialDataset.col(BpmnaiVariables.VAR_PROCESS_INSTANCE_VARIABLE_NAME).isNull();
        }

        if(config.getDataLevel().equals(BpmnaiVariables.DATA_LEVEL_PROCESS)) {
            initialDataset = initialDataset
                    .select(selectionColumns)
                    .filter(filter)
                    .groupBy(BpmnaiVariables.VAR_PROCESS_INSTANCE_ID)
                    .agg(aggregationMap)
                    .withColumnRenamed(BpmnaiVariables.VAR_PROCESS_INSTANCE_ID, BpmnaiVariables.VAR_PROCESS_INSTANCE_ID+"_right");
        } else {
            initialDataset = initialDataset
                    .select(selectionColumns)
                    .filter(initialDataset.col(BpmnaiVariables.VAR_ACT_ID).isNotNull())
                    .groupBy(BpmnaiVariables.VAR_PROCESS_INSTANCE_ID, BpmnaiVariables.VAR_ACT_INST_ID)
                    .agg(aggregationMap)
                    .withColumnRenamed(BpmnaiVariables.VAR_PROCESS_INSTANCE_ID, BpmnaiVariables.VAR_PROCESS_INSTANCE_ID+"_right")
                    .withColumnRenamed(BpmnaiVariables.VAR_ACT_INST_ID, BpmnaiVariables.VAR_ACT_INST_ID+"_right");
        }


        //rename back columns after aggregation
        String pattern = "(first)\\((.+)\\)";
        Pattern r = Pattern.compile(pattern);

        for(String columnName : initialDataset.columns()) {
            Matcher m = r.matcher(columnName);
            if(m.find()) {
                String newColumnName = m.group(2);
                initialDataset = initialDataset.withColumnRenamed(columnName, newColumnName);
            }
        }

        // rejoin removed columns to dataset
        if(config.getDataLevel().equals(BpmnaiVariables.DATA_LEVEL_PROCESS)) {
            dataset = dataset.join(initialDataset,
                    dataset.col(BpmnaiVariables.VAR_PROCESS_INSTANCE_ID).equalTo(initialDataset.col(BpmnaiVariables.VAR_PROCESS_INSTANCE_ID+"_right")
                    ), "left");
        } else {
            dataset = dataset.join(initialDataset,
                    dataset.col(BpmnaiVariables.VAR_PROCESS_INSTANCE_ID).equalTo(initialDataset.col(BpmnaiVariables.VAR_PROCESS_INSTANCE_ID+"_right"))
                            .and(dataset.col(BpmnaiVariables.VAR_ACT_INST_ID).equalTo(initialDataset.col(BpmnaiVariables.VAR_ACT_INST_ID+"_right")))
                    , "left");
        }

        if(config.isDevProcessStateColumnWorkaroundEnabled() && config.getDataLevel().equals(BpmnaiVariables.DATA_LEVEL_PROCESS)) {
            dataset = dataset.drop(BpmnaiVariables.VAR_PROCESS_INSTANCE_VARIABLE_NAME);
        }

        dataset = dataset.drop(BpmnaiVariables.VAR_PROCESS_INSTANCE_ID+"_right");
        if(config.getDataLevel().equals(BpmnaiVariables.DATA_LEVEL_ACTIVITY)) {
            dataset = dataset.drop(BpmnaiVariables.VAR_ACT_INST_ID+"_right");
        }

        if(config.isWriteStepResultsIntoFile()) {
            BpmnaiUtils.getInstance().writeDatasetToCSV(dataset, "joined_columns", config);
        }

        //return preprocessed data
        return dataset;
    }
}
