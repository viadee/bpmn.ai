package de.viadee.ki.sparkimporter.processing.interfaces;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public interface PreprocessingStepInterface {

    /**
     * Defines one processing step and what to do with the data
     *
     * @param dataSet the incoming dataset for this processing step
     * @return the resulting dataset of the processing step
     */
    Dataset<Row> runPreprocessingStep(Dataset<Row> dataSet, boolean writeStepResultIntoFile);
}
