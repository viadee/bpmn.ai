package de.viadee.ki.sparkimporter.processing.steps.dataprocessing;

import de.viadee.ki.sparkimporter.annotation.PreprocessingStepDescription;
import de.viadee.ki.sparkimporter.processing.interfaces.PreprocessingStepInterface;
import de.viadee.ki.sparkimporter.util.SparkBroadcastHelper;
import de.viadee.ki.sparkimporter.util.SparkImporterUtils;
import de.viadee.ki.sparkimporter.util.SparkImporterVariables;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.Map;
import java.util.Set;

import static org.apache.spark.sql.functions.when;

@PreprocessingStepDescription(value = "For each variable a respective column is added and the data for each process instance is filled accordingly.")
public class AddVariablesColumnsStep implements PreprocessingStepInterface {

    @Override
    public Dataset<Row> runPreprocessingStep(Dataset<Row> dataset, boolean writeStepResultIntoFile, String dataLevel, Map<String, Object> parameters) {

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
