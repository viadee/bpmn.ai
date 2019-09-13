package de.viadee.ki.sparkimporter.processing.steps.importing;

import de.viadee.ki.sparkimporter.processing.interfaces.PreprocessingStepInterface;
import de.viadee.ki.sparkimporter.runner.config.SparkRunnerConfig;
import de.viadee.ki.sparkimporter.util.SparkImporterUtils;
import de.viadee.ki.sparkimporter.util.SparkImporterVariables;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;

import java.util.Map;

public class InitialCleanupStep implements PreprocessingStepInterface {
    @Override
    public Dataset<Row> runPreprocessingStep(Dataset<Row> dataset, Map<String, Object> parameters, SparkRunnerConfig config) {

        // remove duplicated columns created at CSV import step
        dataset = SparkImporterUtils.getInstance().removeDuplicatedColumns(dataset);

        //after a CSV import it can occur that we have completely empty lines. This step will remove those
        dataset = SparkImporterUtils.getInstance().removeEmptyLinesAfterImport(dataset);

        dataset = dataset.withColumn(SparkImporterVariables.VAR_DATA_SOURCE, functions.lit("variableUpdate"));

        // write imported unique column CSV structure to file for debugging
        if (config.isWriteStepResultsIntoFile()) {
            SparkImporterUtils.getInstance().writeDatasetToCSV(dataset, "import_unique_columns_result", config);
        }

        return dataset;
    }
}
