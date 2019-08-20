package de.viadee.ki.sparkimporter.processing.steps.userconfig;

import de.viadee.ki.sparkimporter.processing.interfaces.PreprocessingStepInterface;
import de.viadee.ki.sparkimporter.runner.config.SparkRunnerConfig;
import de.viadee.ki.sparkimporter.util.helper.SparkBroadcastHelper;
import de.viadee.ki.sparkimporter.util.logging.SparkImporterLogger;
import de.viadee.ki.sparkimporter.util.SparkImporterUtils;
import de.viadee.ki.sparkimporter.util.SparkImporterVariables;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.col;

/**
 *  This ProcessingStep returns a DataSet which comprises of all activities and variable updates that took place
 *  before the first occurrence of a given activity. The id of the activity for which the dataset should be filtered
 *  for is passed via the applications configuration file.
 */
public class DataFilterOnActivityStep implements PreprocessingStepInterface {
    /**
     * @param dataSet the incoming dataset for this processing step
     * @param parameters
     * @return the filtered DataSet
     */
    @Override
    public Dataset<Row> runPreprocessingStep(Dataset<Row> dataSet, Map<String, Object> parameters, SparkRunnerConfig config) {
        // any parameters set?
        if (parameters == null || parameters.size() == 0) {
            SparkImporterLogger.getInstance().writeWarn("No parameters found for the DataFilterOnActivityStep");
            return dataSet;
        }

        // get query parameter
        String query = (String) parameters.get("query");
        SparkImporterLogger.getInstance().writeInfo("Filtering data with activity instance filter query: " + query + ".");

        // save size of initial dataset for log
        Long initialDSCount = dataSet.count();

        // repartition by process instance and order by start_time for this operation
        dataSet = dataSet.repartition(dataSet.col(SparkImporterVariables.VAR_PROCESS_INSTANCE_ID)).sortWithinPartitions(SparkImporterVariables.VAR_START_TIME);

        // we temporarily store variable updates (rows with a var type set) separately.
        Dataset<Row> variables = dataSet.filter(col(SparkImporterVariables.VAR_PROCESS_INSTANCE_VARIABLE_TYPE).isNotNull());
        //find first occurrence of activity instance
        final Dataset<Row> dsTmp = dataSet.filter(dataSet.col(SparkImporterVariables.VAR_ACT_ID).equalTo(query)).filter(dataSet.col(SparkImporterVariables.VAR_END_TIME).isNull()); //TODO: ENSURING THAT THIS ISN'T A VARIABLE ROW

        // now we look for the first occurrence of the activity id contained in "query". The result comprises of a dataset of corresponding activity instances.
        final Dataset<Row> dsActivityInstances = dataSet.filter(dataSet.col(SparkImporterVariables.VAR_ACT_ID).like(query)).filter(dataSet.col(SparkImporterVariables.VAR_END_TIME).isNull()); //TODO: ENSURING THAT THIS ISN'T A VARIABLE ROW

        // we slim the resulting dataset down: only the activity instances process id and the instances start time are relevant.
        List<Row> activityRows = dsActivityInstances.select(SparkImporterVariables.VAR_PROCESS_INSTANCE_ID, SparkImporterVariables.VAR_START_TIME).collectAsList();
        Map<String, String> activities = activityRows.stream().collect(Collectors.toMap(
                r -> r.getAs(SparkImporterVariables.VAR_PROCESS_INSTANCE_ID), r -> r.getAs(SparkImporterVariables.VAR_START_TIME)));
        // broadcasting the PID - Start time Map to use it in a user defined function
        SparkBroadcastHelper.getInstance().broadcastVariable(SparkBroadcastHelper.BROADCAST_VARIABLE.PROCESS_INSTANCE_TIMESTAMP_MAP, activities);

        // now we have to select for each process instance in our inital dataset all events that happend before the first occurence of our selected activity.
        // We first narrow it down to the process instances in question
        Dataset<Row> selectedProcesses = dataSet.filter(col(SparkImporterVariables.VAR_PROCESS_INSTANCE_ID).isin(activities.keySet().toArray()));
        // Then, we mark all events that should be removed
        Dataset<Row> activityDataSet = selectedProcesses.withColumn("data_filter_on_activity",
                callUDF("activityBeforeTimestamp",
                        selectedProcesses.col(SparkImporterVariables.VAR_PROCESS_INSTANCE_ID),
                        selectedProcesses.col(SparkImporterVariables.VAR_START_TIME)));
        // And we keep the rest
        activityDataSet = activityDataSet.filter(col("data_filter_on_activity").like("TRUE"));
        // Clean up
        activityDataSet = activityDataSet.drop("data_filter_on_activity");

        // However, we lost all variable updates in this approach, so now we add the variables in question to the dataset
        // first, we narrow it down to keep only variables that have a corresponding activity instance
        activityDataSet = activityDataSet.withColumnRenamed(SparkImporterVariables.VAR_ACT_INST_ID, SparkImporterVariables.VAR_ACT_INST_ID+"_RIGHT");

        variables = variables.join(activityDataSet.select(SparkImporterVariables.VAR_ACT_INST_ID+"_RIGHT").distinct(), variables.col(SparkImporterVariables.VAR_ACT_INST_ID).equalTo(activityDataSet.col(SparkImporterVariables.VAR_ACT_INST_ID+"_RIGHT")),"inner");

        activityDataSet = activityDataSet.withColumnRenamed(SparkImporterVariables.VAR_ACT_INST_ID+"_RIGHT", SparkImporterVariables.VAR_ACT_INST_ID);
        variables = variables.drop(SparkImporterVariables.VAR_ACT_INST_ID+"_RIGHT");
        dataSet = activityDataSet.union(variables);
        SparkImporterLogger.getInstance().writeInfo("DataFilterOnActivityStep: The filtered DataSet contains "+dataSet.count()+" rows, (before: "+ initialDSCount+" rows)");

        if (config.isWriteStepResultsIntoFile()) {
            SparkImporterUtils.getInstance().writeDatasetToCSV(dataSet, "data_filter_on_activity_step", config);
        }

        return dataSet;


    }
}
