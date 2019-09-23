package de.viadee.bpmnai.core.processing.interfaces;

import de.viadee.bpmnai.core.runner.config.SparkRunnerConfig;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.Map;

public interface PreprocessingStepInterface {

    /**
     * Defines one processing step and what to do with the data
     *
     * @param dataSet the incoming dataset for this processing step
     * @param config the SparkRunnerConfig for this run
     * @return the resulting dataset of the processing step
     */
    Dataset<Row> runPreprocessingStep(Dataset<Row> dataSet, Map<String, Object> parameters, SparkRunnerConfig config);
}
