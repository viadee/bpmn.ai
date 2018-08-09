package de.viadee.ki.sparkimporter.preprocessing.steps;

import de.viadee.ki.sparkimporter.preprocessing.interfaces.PreprocessingStepInterface;
import de.viadee.ki.sparkimporter.util.SparkImporterCache;
import de.viadee.ki.sparkimporter.util.SparkImporterUtils;
import de.viadee.ki.sparkimporter.util.SparkImporterVariables;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import static org.apache.spark.sql.functions.count;
import static org.apache.spark.sql.functions.max;

public class GetVariablesTypesOccurenceStep implements PreprocessingStepInterface {

    @Override
    public Dataset<Row> runPreprocessingStep(Dataset<Row> dataset, boolean writeStepResultIntoFile) {

        //Determine the process instances with their variable names and types
        Dataset<Row> variablesTypesDataset = dataset.select(SparkImporterVariables.VAR_PROCESS_INSTANCE_VARIABLE_NAME, SparkImporterVariables.VAR_PROCESS_INSTANCE_VARIABLE_TYPE, SparkImporterVariables.VAR_PROCESS_INSTANCE_VARIABLE_REVISION)
                .groupBy(SparkImporterVariables.VAR_PROCESS_INSTANCE_VARIABLE_NAME, SparkImporterVariables.VAR_PROCESS_INSTANCE_VARIABLE_TYPE)
                .agg(max(SparkImporterVariables.VAR_PROCESS_INSTANCE_VARIABLE_REVISION).alias(SparkImporterVariables.VAR_PROCESS_INSTANCE_VARIABLE_REVISION))
                .filter(SparkImporterVariables.VAR_PROCESS_INSTANCE_VARIABLE_NAME+" <> 'null'"); // don't consider null variables'

        variablesTypesDataset.foreach(row -> {
            SparkImporterCache.getInstance().addValueToCache(SparkImporterCache.CACHE_VARIABLE_NAMES_AND_TYPES, row.getString(0), new String[]{row.getString(1), row.getInt(2)+""});
        });

        if(writeStepResultIntoFile) {
            SparkImporterUtils.getInstance().writeDatasetToCSV(variablesTypesDataset, "variables_types_help");
        }

        //return preprocessed data
        return dataset;
    }
}
