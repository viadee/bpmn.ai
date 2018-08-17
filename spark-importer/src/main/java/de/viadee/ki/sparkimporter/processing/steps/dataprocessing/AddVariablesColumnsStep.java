package de.viadee.ki.sparkimporter.processing.steps.dataprocessing;

import de.viadee.ki.sparkimporter.processing.interfaces.PreprocessingStepInterface;
import de.viadee.ki.sparkimporter.util.SparkBroadcastHelper;
import de.viadee.ki.sparkimporter.util.SparkImporterArguments;
import de.viadee.ki.sparkimporter.util.SparkImporterUtils;
import de.viadee.ki.sparkimporter.util.SparkImporterVariables;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.Map;
import java.util.Set;

import static org.apache.spark.sql.functions.when;

public class AddVariablesColumnsStep implements PreprocessingStepInterface {

    @Override
    public Dataset<Row> runPreprocessingStep(Dataset<Row> dataset, boolean writeStepResultIntoFile) {

        Map<String, String> varMap = (Map<String, String>) SparkBroadcastHelper.getInstance().getBroadcastVariable(SparkBroadcastHelper.BROADCAST_VARIABLE.PROCESS_VARIABLES_ESCALATED);
        Set<String> variables = varMap.keySet();

        for(String v : variables) {
            dataset = dataset.select("*").withColumn(v, when(dataset.col(SparkImporterVariables.VAR_PROCESS_INSTANCE_VARIABLE_NAME).equalTo(v),
                    when(dataset.col(SparkImporterVariables.VAR_PROCESS_INSTANCE_VARIABLE_TYPE).equalTo("string"), dataset.col(SparkImporterVariables.VAR_TEXT))
                            .when(dataset.col(SparkImporterVariables.VAR_PROCESS_INSTANCE_VARIABLE_TYPE).equalTo("null"), dataset.col(SparkImporterVariables.VAR_TEXT))
                            .when(dataset.col(SparkImporterVariables.VAR_PROCESS_INSTANCE_VARIABLE_TYPE).equalTo("boolean"), dataset.col(SparkImporterVariables.VAR_LONG))
                            .when(dataset.col(SparkImporterVariables.VAR_PROCESS_INSTANCE_VARIABLE_TYPE).equalTo("long"), dataset.col(SparkImporterVariables.VAR_LONG))
                            .when(dataset.col(SparkImporterVariables.VAR_PROCESS_INSTANCE_VARIABLE_TYPE).equalTo("serializable"), dataset.col(SparkImporterVariables.VAR_TEXT2))
                            .otherwise(""))
                    .otherwise(""));

            if(SparkImporterArguments.getInstance().isRevisionCount()) {
                dataset = dataset.withColumn(v+"_rev",
                        when(dataset.col(SparkImporterVariables.VAR_PROCESS_INSTANCE_VARIABLE_NAME).equalTo(v), dataset.col(SparkImporterVariables.VAR_PROCESS_INSTANCE_VARIABLE_REVISION))
                        .otherwise("0"));
            }
        }

        //drop unnecesssary columns
        dataset = dataset.drop(SparkImporterVariables.VAR_PROCESS_INSTANCE_VARIABLE_NAME,
                SparkImporterVariables.VAR_PROCESS_INSTANCE_VARIABLE_TYPE,
                SparkImporterVariables.VAR_PROCESS_INSTANCE_VARIABLE_REVISION,
                SparkImporterVariables.VAR_DOUBLE,
                SparkImporterVariables.VAR_LONG,
                SparkImporterVariables.VAR_TEXT,
                SparkImporterVariables.VAR_TEXT2);

        if(writeStepResultIntoFile) {
            SparkImporterUtils.getInstance().writeDatasetToCSV(dataset, "add_var_columns");
        }

        //return preprocessed data
        return dataset;
    }
}
