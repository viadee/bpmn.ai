package de.viadee.ki.sparkimporter.processing.steps.dataprocessing;

import de.viadee.ki.sparkimporter.annotation.PreprocessingStepDescription;
import de.viadee.ki.sparkimporter.processing.interfaces.PreprocessingStepInterface;
import de.viadee.ki.sparkimporter.runner.config.SparkRunnerConfig;
import de.viadee.ki.sparkimporter.util.logging.SparkImporterLogger;
import de.viadee.ki.sparkimporter.util.SparkImporterUtils;
import de.viadee.ki.sparkimporter.util.SparkImporterVariables;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.spark.sql.functions.isnull;
import static org.apache.spark.sql.functions.not;

@PreprocessingStepDescription(name = "Aggregate process instances", description = "In this step the data is aggregated in a way so that there is only one line per process instance in the dataset. In this step the process state for each process instance is also aggregated to the last state the process instance had in the underlying dataset.")
public class AggregateProcessInstancesStep implements PreprocessingStepInterface {

    @Override
    public Dataset<Row> runPreprocessingStep(Dataset<Row> dataset, boolean writeStepResultIntoFile, String dataLevel, Map<String, Object> parameters, SparkRunnerConfig config) {

        //apply first and processState aggregator
        Map<String, String> aggregationMap = new HashMap<>();
        for(String column : dataset.columns()) {
            if(column.equals(SparkImporterVariables.VAR_PROCESS_INSTANCE_ID)) {
                continue;
            } else if(column.equals(SparkImporterVariables.VAR_DURATION)) {
                aggregationMap.put(column, "max");
            } else if(column.equals(SparkImporterVariables.VAR_STATE)) {
                aggregationMap.put(column, "ProcessState");
            } else {
                aggregationMap.put(column, "AllButEmptyString");
            }
        }

        Column filter = not(isnull(dataset.col(SparkImporterVariables.VAR_STATE)));
        if(config.isDevProcessStateColumnWorkaroundEnabled() && dataLevel.equals(SparkImporterVariables.DATA_LEVEL_PROCESS)) {
            filter = isnull(dataset.col(SparkImporterVariables.VAR_PROCESS_INSTANCE_VARIABLE_NAME));
        }

        //first aggregation
        Dataset<Row> datasetPIAgg = dataset
                .filter(filter)
                .groupBy(SparkImporterVariables.VAR_PROCESS_INSTANCE_ID)
                .agg(aggregationMap);

        //rename back columns after aggregation
        String pattern = "(max|allbutemptystring|processstate)\\((.+)\\)";
        Pattern r = Pattern.compile(pattern);

        for(String columnName : dataset.columns()) {
            Matcher m = r.matcher(columnName);
            if(m.find()) {
                String newColumnName = m.group(2);
                dataset = dataset.withColumnRenamed(columnName, newColumnName);
            }
        }

        filter = isnull(dataset.col(SparkImporterVariables.VAR_STATE));
        if(config.isDevProcessStateColumnWorkaroundEnabled() && dataLevel.equals(SparkImporterVariables.DATA_LEVEL_PROCESS)) {
            filter = not(isnull(dataset.col(SparkImporterVariables.VAR_PROCESS_INSTANCE_VARIABLE_NAME)));
        }

        dataset = dataset
                .filter(filter)
                .groupBy(SparkImporterVariables.VAR_PROCESS_INSTANCE_ID)
                .agg(aggregationMap)
                .union(datasetPIAgg);

        //rename back columns after aggregation
        for(String columnName : dataset.columns()) {
            Matcher m = r.matcher(columnName);
            if(m.find()) {
                String newColumnName = m.group(2);
                dataset = dataset.withColumnRenamed(columnName, newColumnName);
            }
        }

        //in case we add the CSV we have a name column in the first dataset of the join so we call drop again to make sure it is gone
        dataset = dataset.drop(SparkImporterVariables.VAR_PROCESS_INSTANCE_VARIABLE_NAME);
        dataset = dataset.drop(SparkImporterVariables.VAR_ACT_INST_ID);

        SparkImporterLogger.getInstance().writeInfo("Found " + dataset.count() + " process instances.");

        if(writeStepResultIntoFile) {
            SparkImporterUtils.getInstance().writeDatasetToCSV(dataset, "agg_of_process_instances", config);
        }

        //return preprocessed data
        return dataset;
    }
}
