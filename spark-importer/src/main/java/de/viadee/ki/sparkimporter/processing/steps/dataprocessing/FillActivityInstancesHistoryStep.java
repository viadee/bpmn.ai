package de.viadee.ki.sparkimporter.processing.steps.dataprocessing;

import de.viadee.ki.sparkimporter.annotation.PreprocessingStepDescription;
import de.viadee.ki.sparkimporter.processing.interfaces.PreprocessingStepInterface;
import de.viadee.ki.sparkimporter.runner.config.SparkRunnerConfig;
import de.viadee.ki.sparkimporter.util.SparkImporterUtils;
import de.viadee.ki.sparkimporter.util.SparkImporterVariables;
import de.viadee.ki.sparkimporter.util.helper.SparkBroadcastHelper;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.apache.spark.sql.functions.when;

@PreprocessingStepDescription(name = "Fill activity instances history", description = "In this step each variable column is filled with values according to the history of the process instance up to the point of activity activity represented in the line.")
public class FillActivityInstancesHistoryStep implements PreprocessingStepInterface {

    @Override
    public Dataset<Row> runPreprocessingStep(Dataset<Row> dataset, Map<String, Object> parameters, SparkRunnerConfig config) {

        // get variables
        Map<String, String> varMap = (Map<String, String>) SparkBroadcastHelper.getInstance().getBroadcastVariable(SparkBroadcastHelper.BROADCAST_VARIABLE.PROCESS_VARIABLES_ESCALATED);

        //convert to String array so it is serializable and can be used in map function
        Set<String> variables = varMap.keySet();
        String[] vars = new String[variables.size()];
        int vc = 0;
        for(String v : variables) {
            vars[vc++] = v;
        }

        // make empty values actually null
        for(String v : variables) {
            if(Arrays.asList(dataset.columns()).contains(v)) {
                dataset = dataset.withColumn(v, when(dataset.col(v).equalTo(""), null).otherwise(dataset.col(v)));
            }
        }

        Map<String, String> valuesToWrite = new HashMap<>();
        final String[] lastProcessInstanceId = {""};
        String[] columns = dataset.columns();

        //repartition py process instance and order by start_time for this operation
        dataset = dataset.repartition(dataset.col(SparkImporterVariables.VAR_PROCESS_INSTANCE_ID)).sortWithinPartitions(SparkImporterVariables.VAR_START_TIME);

        //iterate through dataset and fill up values in each process instance
        dataset = dataset.map(row -> {
            String currentProcessInstanceId = row.getAs(SparkImporterVariables.VAR_PROCESS_INSTANCE_ID);
            String[] newRow = new String[columns.length];

            //check if we switch to a new process instance
            if(!lastProcessInstanceId[0].equals(currentProcessInstanceId)) {
                // new process instance
                valuesToWrite.clear();
                lastProcessInstanceId[0] = currentProcessInstanceId;
            }

            int columnCount = 0;
            for(String c : columns) {
                String columnValue = null;
                if(Arrays.asList(vars).contains(c)) {
                    //it was a variable
                    if(valuesToWrite.get(c) != null) {
                        // we already have a value for the current process instance, so we use it
                        columnValue = valuesToWrite.get(c);
                    } else {
                        // we don't have a value yet for the current process instance
                        String currentValue = row.getAs(c);
                        if(currentValue != null) {
                            valuesToWrite.put(c, currentValue);
                            columnValue = currentValue;
                        }
                    }
                } else {
                    //it was a column, just use the value as it is
                    columnValue = row.getAs(c);
                }
                newRow[columnCount++] = columnValue;
            }

            return RowFactory.create(newRow);
        }, RowEncoder.apply(dataset.schema()));

        if(config.isWriteStepResultsIntoFile()) {
            SparkImporterUtils.getInstance().writeDatasetToCSV(dataset, "fill_activity_instances_history", config);
        }

        //return preprocessed data
        return dataset;
    }
}
