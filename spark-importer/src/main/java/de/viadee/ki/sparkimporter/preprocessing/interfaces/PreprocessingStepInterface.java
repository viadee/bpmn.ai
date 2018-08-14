package de.viadee.ki.sparkimporter.preprocessing.interfaces;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public interface PreprocessingStepInterface {

    /**
     * Defines one preprocessing step and what to do with the data
     *
     * @param dataSet the incoming dataset for this preprocessing step
     * @return the resulting dataset of the preprocessing step
     */
    Dataset<Row> runPreprocessingStep(Dataset<Row> dataSet, boolean writeStepResultIntoFile);
}
