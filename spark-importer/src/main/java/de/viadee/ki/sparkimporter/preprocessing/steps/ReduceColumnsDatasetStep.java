package de.viadee.ki.sparkimporter.preprocessing.steps;

import de.viadee.ki.sparkimporter.preprocessing.interfaces.PreprocessingStepInterface;
import de.viadee.ki.sparkimporter.util.SparkImporterUtils;
import de.viadee.ki.sparkimporter.util.SparkImporterVariables;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class ReduceColumnsDatasetStep implements PreprocessingStepInterface {

    @Override
    public Dataset<Row> runPreprocessingStep(Dataset<Row> dataset, boolean writeStepResultIntoFile) {

        dataset = dataset.select(SparkImporterVariables.VAR_PROCESS_INSTANCE_ID,
                SparkImporterVariables.VAR_PROCESS_INSTANCE_VARIABLE_NAME,
                SparkImporterVariables.VAR_PROCESS_INSTANCE_VARIABLE_TYPE,
                SparkImporterVariables.VAR_PROCESS_INSTANCE_VARIABLE_REVISION,
                SparkImporterVariables.VAR_LONG,
                SparkImporterVariables.VAR_TEXT,
                SparkImporterVariables.VAR_TEXT2)
                .filter(SparkImporterVariables.VAR_PROCESS_INSTANCE_ID + " <> 'null'");

        if(writeStepResultIntoFile) {
            SparkImporterUtils.getInstance().writeDatasetToCSV(dataset, "reduced_columns");
        }

        //return preprocessed data
        return dataset;
    }
}
