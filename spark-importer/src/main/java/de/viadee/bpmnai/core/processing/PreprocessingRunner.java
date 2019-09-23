package de.viadee.bpmnai.core.processing;

import de.viadee.bpmnai.core.processing.steps.PipelineStep;
import de.viadee.bpmnai.core.runner.config.SparkRunnerConfig;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PreprocessingRunner {

    private final List<PipelineStep> pipelineSteps = new ArrayList<>();

    public final static String DATASET_INITIAL = "initial";

    public final static Map<String, Dataset<Row>> helper_datasets = new HashMap<>();

    public PreprocessingRunner(){}

    public Dataset<Row> run(Dataset<Row> dataset, SparkRunnerConfig config) {
        helper_datasets.clear();
        helper_datasets.put(DATASET_INITIAL + "_" + config.getDataLevel(), dataset);

        for(PipelineStep ps : this.pipelineSteps) {
            if(ps.getPreprocessingStep() != null)
            dataset = ps.getPreprocessingStep().runPreprocessingStep(dataset, ps.getStepParameters(), config);
        }
        return dataset;
    }

    public void addPreprocessorStep(PipelineStep step) {
        this.pipelineSteps.add(step);
    }
}
