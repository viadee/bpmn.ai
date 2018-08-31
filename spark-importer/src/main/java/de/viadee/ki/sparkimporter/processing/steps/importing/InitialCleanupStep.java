package de.viadee.ki.sparkimporter.processing.steps.importing;

import de.viadee.ki.sparkimporter.processing.interfaces.PreprocessingStepInterface;
import de.viadee.ki.sparkimporter.util.SparkImporterCSVArguments;
import de.viadee.ki.sparkimporter.util.SparkImporterUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class InitialCleanupStep implements PreprocessingStepInterface {
    @Override
    public Dataset<Row> runPreprocessingStep(Dataset<Row> dataset, boolean writeStepResultIntoFile) {

        // remove duplicated columns created at CSV import step
        dataset = SparkImporterUtils.getInstance().removeDuplicatedColumns(dataset);

        //after a CSV import it can occur that we have completely empty lines. This step will remove those
        dataset = SparkImporterUtils.getInstance().removeEmptyLinesAfterImport(dataset);

        // write imported unique column CSV structure to file for debugging
        if (SparkImporterCSVArguments.getInstance().isWriteStepResultsToCSV()) {
            SparkImporterUtils.getInstance().writeDatasetToCSV(dataset, "import_unique_columns_result");
        }

        return dataset;
    }
}
