package de.viadee.ki.sparkimporter.runner;

import de.viadee.ki.sparkimporter.processing.steps.PipelineStep;
import de.viadee.ki.sparkimporter.processing.steps.dataprocessing.CreateColumnsFromJsonStep;
import de.viadee.ki.sparkimporter.processing.steps.importing.ColumnsPreparationStep;
import de.viadee.ki.sparkimporter.processing.steps.userconfig.ColumnHashStep;
import de.viadee.ki.sparkimporter.processing.steps.userconfig.TypeCastStep;
import de.viadee.ki.sparkimporter.util.SparkImporterLogger;
import de.viadee.ki.sparkimporter.util.SparkImporterUtils;
import de.viadee.ki.sparkimporter.util.SparkImporterVariables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class KafkaProcessingPredictionServiceRunner extends SparkPredictionServiceRunner {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaProcessingPredictionServiceRunner.class);

    @Override
    protected void initialize() {
        //TODO add config
        SparkImporterUtils.setWorkingDirectory("/Users/mim/Desktop/provi_service_config");
        SparkImporterLogger.setLogDirectory("/Users/mim/Desktop/provi_service_config");

        SparkImporterVariables.setPipelineMode(SparkImporterVariables.PIPELINE_MODE_PREDICT);
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
