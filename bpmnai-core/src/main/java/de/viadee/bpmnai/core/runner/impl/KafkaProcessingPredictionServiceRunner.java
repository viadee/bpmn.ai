package de.viadee.bpmnai.core.runner.impl;

import de.viadee.bpmnai.core.processing.steps.PipelineStep;
import de.viadee.bpmnai.core.processing.steps.dataprocessing.ColumnHashStep;
import de.viadee.bpmnai.core.processing.steps.dataprocessing.CreateColumnsFromJsonStep;
import de.viadee.bpmnai.core.processing.steps.dataprocessing.TypeCastStep;
import de.viadee.bpmnai.core.processing.steps.importing.ColumnsPreparationStep;
import de.viadee.bpmnai.core.runner.SparkPredictionServiceRunner;
import de.viadee.bpmnai.core.util.BpmnaiVariables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class KafkaProcessingPredictionServiceRunner extends SparkPredictionServiceRunner {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaProcessingPredictionServiceRunner.class);

    @Override
    public void initialize() {
        this.sparkRunnerConfig.setPipelineMode(BpmnaiVariables.PIPELINE_MODE_PREDICT);
    }

    @Override
    protected List<PipelineStep> buildDefaultPipeline() {
        List<PipelineStep> pipelineSteps = new ArrayList<>();

        // remove duplicated columns created at CSV import step
        pipelineSteps.add(new PipelineStep(new ColumnsPreparationStep(), ""));
        pipelineSteps.add(new PipelineStep(new CreateColumnsFromJsonStep(), "ColumnsPreparationStep"));
        pipelineSteps.add(new PipelineStep(new ColumnHashStep(), "CreateColumnsFromJsonStep"));
        pipelineSteps.add(new PipelineStep(new TypeCastStep(), "ColumnHashStep"));

        return pipelineSteps;
    }
}
