package de.viadee.ki.sparkimporter.preprocessing.steps;

import de.viadee.ki.sparkimporter.preprocessing.interfaces.PreprocessingStepInterface;
import de.viadee.ki.sparkimporter.util.SparkImporterUtils;
import de.viadee.ki.sparkimporter.util.SparkImporterVariables;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import static org.apache.spark.sql.functions.count;
import static org.apache.spark.sql.functions.desc;

public class GetVariablesCountStep implements PreprocessingStepInterface {

    @Override
    public Dataset<Row> runPreprocessingStep(Dataset<Row> dataset, boolean writeStepResultIntoFile) {

        //Determine number of process variables per instance and start the ones having the most
        Dataset<Row> variablesCountDataset = dataset.groupBy(SparkImporterVariables.VAR_PROCESS_INSTANCE_ID)
                .agg(count(SparkImporterVariables.VAR_PROCESS_INSTANCE_VARIABLE_NAME).alias("variables_count")).
                        orderBy(desc("variables_count"));

        if(writeStepResultIntoFile) {
            SparkImporterUtils.getInstance().writeDatasetToCSV(variablesCountDataset, "variables_count_help");
        }

        //just analysing the data
        return dataset;
    }
}
