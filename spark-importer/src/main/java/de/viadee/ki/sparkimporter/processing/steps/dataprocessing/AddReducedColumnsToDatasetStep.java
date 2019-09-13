package de.viadee.ki.sparkimporter.processing.steps.dataprocessing;

import de.viadee.ki.sparkimporter.annotation.PreprocessingStepDescription;
import de.viadee.ki.sparkimporter.processing.PreprocessingRunner;
import de.viadee.ki.sparkimporter.processing.interfaces.PreprocessingStepInterface;
import de.viadee.ki.sparkimporter.runner.config.SparkRunnerConfig;
import de.viadee.ki.sparkimporter.util.SparkImporterUtils;
import de.viadee.ki.sparkimporter.util.SparkImporterVariables;
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
                SparkImporterVariables.VAR_PROCESS_INSTANCE_VARIABLE_TYPE,
                SparkImporterVariables.VAR_PROCESS_INSTANCE_VARIABLE_REVISION,
                SparkImporterVariables.VAR_LONG,
                SparkImporterVariables.VAR_DOUBLE,
                SparkImporterVariables.VAR_TEXT,
                SparkImporterVariables.VAR_TEXT2,
                SparkImporterVariables.VAR_TIMESTAMP,
                SparkImporterVariables.VAR_SEQUENCE_COUNTER,
                SparkImporterVariables.VAR_PROCESS_INSTANCE_VARIABLE_INSTANCE_ID,
                SparkImporterVariables.VAR_DATA_SOURCE
        });

        if(!config.isDevProcessStateColumnWorkaroundEnabled()) {
            columnsNotBeAddedAgain = Stream.concat(columnsNotBeAddedAgain.stream(), Stream.of(SparkImporterVariables.VAR_PROCESS_INSTANCE_VARIABLE_NAME)).collect(Collectors.toList());
        }

        columnNames.add(new Column(SparkImporterVariables.VAR_PROCESS_INSTANCE_ID));
        columnNames.add(new Column(SparkImporterVariables.VAR_STATE));
        columnNames.add(new Column(SparkImporterVariables.VAR_ACT_INST_ID));
        for(Row row : startColumns.collectAsList()) {
            String column = row.getString(0);
            if(!existingColumns.contains(column) && !columnsNotBeAddedAgain.contains(column)) {
                columnNamesString.add(column);
                columnNames.add(new Column(column));
            }
        }
        Seq<Column> selectionColumns = SparkImporterUtils.getInstance().asSeq(columnNames);

        //get relevant data from initial dataset to be added back again
        Dataset<Row> initialDataset = PreprocessingRunner.helper_datasets.get(PreprocessingRunner.DATASET_INITIAL + "_" + config.getDataLevel());
        Map<String, String> aggregationMap = new HashMap<>();
        for(String column : columnNamesString) {
            aggregationMap.put(column, "first");
        }

        Column filter = initialDataset.col(SparkImporterVariables.VAR_STATE).isNotNull();
        if(config.isDevProcessStateColumnWorkaroundEnabled() && config.getDataLevel().equals(SparkImporterVariables.DATA_LEVEL_PROCESS)) {
            filter = initialDataset.col(SparkImporterVariables.VAR_PROCESS_INSTANCE_VARIABLE_NAME).isNull();
        }

        if(config.getDataLevel().equals(SparkImporterVariables.DATA_LEVEL_PROCESS)) {
            initialDataset = initialDataset
                    .select(selectionColumns)
                    .filter(filter)
                    .groupBy(SparkImporterVariables.VAR_PROCESS_INSTANCE_ID)
                    .agg(aggregationMap)
                    .withColumnRenamed(SparkImporterVariables.VAR_PROCESS_INSTANCE_ID, SparkImporterVariables.VAR_PROCESS_INSTANCE_ID+"_right");
        } else {
            initialDataset = initialDataset
                    .select(selectionColumns)
                    .filter(initialDataset.col(SparkImporterVariables.VAR_ACT_ID).isNotNull())
                    .groupBy(SparkImporterVariables.VAR_PROCESS_INSTANCE_ID, SparkImporterVariables.VAR_ACT_INST_ID)
                    .agg(aggregationMap)
                    .withColumnRenamed(SparkImporterVariables.VAR_PROCESS_INSTANCE_ID, SparkImporterVariables.VAR_PROCESS_INSTANCE_ID+"_right")
                    .withColumnRenamed(SparkImporterVariables.VAR_ACT_INST_ID, SparkImporterVariables.VAR_ACT_INST_ID+"_right");
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
        if(config.getDataLevel().equals(SparkImporterVariables.DATA_LEVEL_PROCESS)) {
            dataset = dataset.join(initialDataset,
                    dataset.col(SparkImporterVariables.VAR_PROCESS_INSTANCE_ID).equalTo(initialDataset.col(SparkImporterVariables.VAR_PROCESS_INSTANCE_ID+"_right")
                    ), "left");
        } else {
            dataset = dataset.join(initialDataset,
                    dataset.col(SparkImporterVariables.VAR_PROCESS_INSTANCE_ID).equalTo(initialDataset.col(SparkImporterVariables.VAR_PROCESS_INSTANCE_ID+"_right"))
                            .and(dataset.col(SparkImporterVariables.VAR_ACT_INST_ID).equalTo(initialDataset.col(SparkImporterVariables.VAR_ACT_INST_ID+"_right")))
                    , "left");
        }

        if(config.isDevProcessStateColumnWorkaroundEnabled() && config.getDataLevel().equals(SparkImporterVariables.DATA_LEVEL_PROCESS)) {
            dataset = dataset.drop(SparkImporterVariables.VAR_PROCESS_INSTANCE_VARIABLE_NAME);
        }

        dataset = dataset.drop(SparkImporterVariables.VAR_PROCESS_INSTANCE_ID+"_right");
        if(config.getDataLevel().equals(SparkImporterVariables.DATA_LEVEL_ACTIVITY)) {
            dataset = dataset.drop(SparkImporterVariables.VAR_ACT_INST_ID+"_right");
        }

        if(config.isWriteStepResultsIntoFile()) {
            SparkImporterUtils.getInstance().writeDatasetToCSV(dataset, "joined_columns", config);
        }

        //return preprocessed data
        return dataset;
    }
}
