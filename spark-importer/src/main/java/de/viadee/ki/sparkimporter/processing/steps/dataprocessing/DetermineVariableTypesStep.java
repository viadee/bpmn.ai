package de.viadee.ki.sparkimporter.processing.steps.dataprocessing;

import de.viadee.ki.sparkimporter.processing.interfaces.PreprocessingStepInterface;
import de.viadee.ki.sparkimporter.util.SparkBroadcastHelper;
import de.viadee.ki.sparkimporter.util.SparkImporterUtils;
import de.viadee.ki.sparkimporter.util.SparkImporterVariables;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import static org.apache.spark.sql.functions.max;

public class DetermineVariableTypesStep implements PreprocessingStepInterface {

    @Override
    public Dataset<Row> runPreprocessingStep(Dataset<Row> dataset, boolean writeStepResultIntoFile, String dataLevel, Map<String, Object> parameters) {

        //Determine the process instances with their variable names and types
        Dataset<Row> variablesTypesDataset = dataset.select(SparkImporterVariables.VAR_PROCESS_INSTANCE_VARIABLE_NAME, SparkImporterVariables.VAR_PROCESS_INSTANCE_VARIABLE_TYPE, SparkImporterVariables.VAR_PROCESS_INSTANCE_VARIABLE_REVISION)
                .groupBy(SparkImporterVariables.VAR_PROCESS_INSTANCE_VARIABLE_NAME, SparkImporterVariables.VAR_PROCESS_INSTANCE_VARIABLE_TYPE)
                .agg(max(SparkImporterVariables.VAR_PROCESS_INSTANCE_VARIABLE_REVISION).alias(SparkImporterVariables.VAR_PROCESS_INSTANCE_VARIABLE_REVISION))
                .filter(SparkImporterVariables.VAR_PROCESS_INSTANCE_VARIABLE_NAME+" <> 'null'"); // don't consider null variables'

        //create broadcast variable for variables list
        Map<String, String> variablesAndTypes = new HashMap<>();
        Iterator<Row> it = variablesTypesDataset.toLocalIterator();
        while(it.hasNext()) {
            Row row = it.next();
            variablesAndTypes.put(row.getString(0), row.getString(1));
        }

        //broadcast variable in Spark
        SparkBroadcastHelper.getInstance().broadcastVariable(SparkBroadcastHelper.BROADCAST_VARIABLE.PROCESS_VARIABLES_RAW, variablesAndTypes);


        if(writeStepResultIntoFile) {
            SparkImporterUtils.getInstance().writeDatasetToCSV(variablesTypesDataset, "variables_types_help");
        }

        //return preprocessed data
        return dataset;
    }
}
