package de.viadee.ki.sparkimporter.preprocessing.steps.output;

import de.viadee.ki.sparkimporter.preprocessing.interfaces.PreprocessingStepInterface;
import de.viadee.ki.sparkimporter.util.SparkImporterUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class WriteToCSVStep implements PreprocessingStepInterface {
    @Override
    public Dataset<Row> runPreprocessingStep(Dataset<Row> dataset, boolean writeStepResultIntoFile) {

        SparkImporterUtils.getInstance().writeDatasetToCSV(dataset, "result");

        return dataset;
    }
}
