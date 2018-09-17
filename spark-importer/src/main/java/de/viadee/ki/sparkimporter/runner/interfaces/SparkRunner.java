package de.viadee.ki.sparkimporter.runner.interfaces;

import de.viadee.ki.sparkimporter.configuration.Configuration;
import de.viadee.ki.sparkimporter.configuration.preprocessing.CustomStep;
import de.viadee.ki.sparkimporter.configuration.preprocessing.PipelineStepConfiguration;
import de.viadee.ki.sparkimporter.configuration.preprocessing.PreprocessingConfiguration;
import de.viadee.ki.sparkimporter.configuration.util.ConfigurationUtils;
import de.viadee.ki.sparkimporter.exceptions.FaultyConfigurationException;
import de.viadee.ki.sparkimporter.processing.steps.PipelineManager;
import de.viadee.ki.sparkimporter.processing.steps.PipelineStep;
import org.apache.spark.sql.SparkSession;

import java.util.ArrayList;
import java.util.List;

public abstract class SparkRunner {

    List<PipelineStep> pipelineSteps = new ArrayList<>();

    protected abstract void run(SparkSession sparkSession);

    public void configureCustomSteps() throws FaultyConfigurationException {

        List<CustomStep> customSteps = null;

        Configuration configuration = ConfigurationUtils.getInstance().getConfiguration();
        if(configuration != null) {
            PreprocessingConfiguration preprocessingConfiguration = configuration.getPreprocessingConfiguration();
            if (preprocessingConfiguration != null) {
                PipelineStepConfiguration pipelineStepConfiguration = preprocessingConfiguration.getPipelineStepConfiguration();
                if(pipelineStepConfiguration != null) {
                    customSteps = pipelineStepConfiguration.getCustomSteps();
                }
            }
        }

        // add custom steps to pipeline
        if(customSteps != null) {
            for(CustomStep cs : customSteps) {
                pipelineSteps.add(new PipelineStep(cs));
            }

            PipelineManager pipelineManager = new PipelineManager(pipelineSteps);
            }
    }
}
