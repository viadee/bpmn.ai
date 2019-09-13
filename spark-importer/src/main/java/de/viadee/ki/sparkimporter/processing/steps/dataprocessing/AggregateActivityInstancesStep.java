package de.viadee.ki.sparkimporter.processing.steps.dataprocessing;

import de.viadee.ki.sparkimporter.annotation.PreprocessingStepDescription;
import de.viadee.ki.sparkimporter.processing.interfaces.PreprocessingStepInterface;
import de.viadee.ki.sparkimporter.runner.config.SparkRunnerConfig;
import de.viadee.ki.sparkimporter.runner.impl.KafkaImportRunner;
import de.viadee.ki.sparkimporter.util.SparkImporterUtils;
import de.viadee.ki.sparkimporter.util.SparkImporterVariables;
import de.viadee.ki.sparkimporter.util.logging.SparkImporterLogger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@PreprocessingStepDescription(name = "Aggregate activity instances", description = "In this step the data is aggregated in a way so that there is only one line per activity instance per process instance in the dataset.")
public class AggregateActivityInstancesStep implements PreprocessingStepInterface {

    @Override
    public Dataset<Row> runPreprocessingStep(Dataset<Row> dataset, Map<String, Object> parameters, SparkRunnerConfig config) {

        //apply first and processState aggregator
        Map<String, String> aggregationMap = new HashMap<>();
        for(String column : dataset.columns()) {
            if(column.equals(SparkImporterVariables.VAR_PROCESS_INSTANCE_ID)) {
                continue;
            } else if(column.equals(SparkImporterVariables.VAR_DURATION) || column.endsWith("_rev")) {
                aggregationMap.put(column, "max");
            } else if(column.equals(SparkImporterVariables.VAR_STATE)) {
                aggregationMap.put(column, "ProcessState");
            } else if(column.equals(SparkImporterVariables.VAR_ACT_INST_ID)) {
                //ignore it, as we aggregate by it
                continue;
            } else {
                aggregationMap.put(column, "AllButEmptyString");
            }
        }

        //first aggregation
        //activity level, take only processInstance and activityInstance rows
        dataset = dataset
                .filter(dataset.col(SparkImporterVariables.VAR_DATA_SOURCE).notEqual(KafkaImportRunner.TOPIC_PROCESS_INSTANCE))
                .groupBy(SparkImporterVariables.VAR_PROCESS_INSTANCE_ID, SparkImporterVariables.VAR_ACT_INST_ID)
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


        //in case we add the CSV we have a name column in the first dataset of the join so we call drop again to make sure it is gone
        dataset = dataset.drop(SparkImporterVariables.VAR_PROCESS_INSTANCE_VARIABLE_NAME);
        dataset = dataset.drop(SparkImporterVariables.VAR_DATA_SOURCE);

        dataset = dataset.sort(SparkImporterVariables.VAR_START_TIME);

        SparkImporterLogger.getInstance().writeInfo("Found " + dataset.count() + " activity instances.");

        if(config.isWriteStepResultsIntoFile()) {
            SparkImporterUtils.getInstance().writeDatasetToCSV(dataset, "agg_of_activity_instances", config);
        }

        //return preprocessed data
        return dataset;
    }
}
