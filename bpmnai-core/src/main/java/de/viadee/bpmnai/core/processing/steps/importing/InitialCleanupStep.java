package de.viadee.bpmnai.core.processing.steps.importing;

import de.viadee.bpmnai.core.processing.interfaces.PreprocessingStepInterface;
import de.viadee.bpmnai.core.runner.config.SparkRunnerConfig;
import de.viadee.bpmnai.core.util.BpmnaiUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.Map;

public class InitialCleanupStep implements PreprocessingStepInterface {
    @Override
    public Dataset<Row> runPreprocessingStep(Dataset<Row> dataset, Map<String, Object> parameters, SparkRunnerConfig config) {

        // remove duplicated columns created at CSV import step
        dataset = BpmnaiUtils.getInstance().removeDuplicatedColumns(dataset);

        //after a CSV import it can occur that we have completely empty lines. This step will remove those
        dataset = BpmnaiUtils.getInstance().removeEmptyLinesAfterImport(dataset);

        // write imported unique column CSV structure to file for debugging
        if (config.isWriteStepResultsIntoFile()) {
            BpmnaiUtils.getInstance().writeDatasetToCSV(dataset, "import_unique_columns_result", config);
        }

        return dataset;
    }
}
