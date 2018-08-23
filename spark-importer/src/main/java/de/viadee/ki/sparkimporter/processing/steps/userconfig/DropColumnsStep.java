package de.viadee.ki.sparkimporter.processing.steps.userconfig;

import de.viadee.ki.sparkimporter.processing.interfaces.PreprocessingStepInterface;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class DropColumnsStep implements PreprocessingStepInterface {
    @Override
    public Dataset<Row> runPreprocessingStep(Dataset<Row> dataSet, boolean writeStepResultIntoFile) {

        return dataSet;
    }
}
