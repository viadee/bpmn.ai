package de.viadee.ki.sparkimporter.runner;

import de.viadee.ki.sparkimporter.configuration.Configuration;
import de.viadee.ki.sparkimporter.configuration.preprocessing.PipelineStepConfiguration;
import de.viadee.ki.sparkimporter.configuration.preprocessing.PreprocessingConfiguration;
import de.viadee.ki.sparkimporter.configuration.preprocessing.Step;
import de.viadee.ki.sparkimporter.configuration.util.ConfigurationUtils;
import de.viadee.ki.sparkimporter.exceptions.FaultyConfigurationException;
import de.viadee.ki.sparkimporter.processing.PreprocessingRunner;
import de.viadee.ki.sparkimporter.processing.steps.PipelineManager;
import de.viadee.ki.sparkimporter.processing.steps.PipelineStep;
import de.viadee.ki.sparkimporter.util.SparkImporterLogger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public abstract class SparkRunner {

    private static final Logger LOG = LoggerFactory.getLogger(SparkRunner.class);

    private PipelineManager pipelineManager = null;
    protected SparkSession sparkSession = null;

    private Dataset<Row> dataset;
    protected String dataLevel = "process";
    private List<PipelineStep> pipelineSteps = new ArrayList<>();

    protected abstract void initialize(String[] arguments);

    protected abstract List<PipelineStep> buildDefaultPipeline();

    protected abstract Dataset<Row> loadInitialDataset();

    private void checkConfig() {
        //if there is no configuration file yet, write one in the next steps
        if(ConfigurationUtils.getInstance().getConfiguration(true) == null) {
            PreprocessingRunner.initialConfigToBeWritten = true;
            ConfigurationUtils.getInstance().createEmptyConfig();
        }
    }

    private void writeConfig() {
        //write initial config file
        if(PreprocessingRunner.initialConfigToBeWritten) {
            ConfigurationUtils.getInstance().writeConfigurationToFile();
        }
    }

    public void run(String[] arguments) throws FaultyConfigurationException {
        // spark configuration is being loaded from Environment (e.g. when using spark-submit)
        sparkSession = SparkSession.builder().getOrCreate();

        initialize(arguments);
        checkConfig();
        configurePipelineSteps();
        dataset = loadInitialDataset();

        //go through pipe elements
        // Define processing steps to run
        final PreprocessingRunner preprocessingRunner = new PreprocessingRunner();

        for(PipelineStep ps : pipelineManager.getOrderedPipeline()) {
            preprocessingRunner.addPreprocessorStep(ps);
        }

        final long startMillis = System.currentTimeMillis();

        // Run processing runner
        preprocessingRunner.run(dataset, dataLevel);

        final long endMillis = System.currentTimeMillis();

        String logMessage = "Job ran for " + ((endMillis - startMillis) / 1000) + " seconds in total";
        LOG.info(logMessage);
        SparkImporterLogger.getInstance().writeInfo(logMessage);

        // Cleanup
        sparkSession.close();

        writeConfig();
    }

    public void configurePipelineSteps() throws FaultyConfigurationException {

        List<Step> steps = null;

        Configuration configuration = ConfigurationUtils.getInstance().getConfiguration();

        if(PreprocessingRunner.initialConfigToBeWritten) {
            pipelineSteps = buildDefaultPipeline();

            PreprocessingConfiguration preprocessingConfiguration = configuration.getPreprocessingConfiguration();
            PipelineStepConfiguration pipelineStepConfiguration = preprocessingConfiguration.getPipelineStepConfiguration();

            List<Step> configSteps = new ArrayList<>();
            for(PipelineStep ps : pipelineSteps) {
                Step s = new Step();
                s.setClassName(ps.getClassName());
                s.setDependsOn(ps.getDependsOn());
                s.setId(ps.getId());
                s.setParameters(ps.getStepParameters());

                configSteps.add(s);
            }

            pipelineStepConfiguration.setSteps(configSteps);
        } else {
            if (configuration != null) {
                PreprocessingConfiguration preprocessingConfiguration = configuration.getPreprocessingConfiguration();
                if (preprocessingConfiguration != null) {
                    PipelineStepConfiguration pipelineStepConfiguration = preprocessingConfiguration.getPipelineStepConfiguration();
                    if (pipelineStepConfiguration != null) {
                        steps = pipelineStepConfiguration.getSteps();

                        if (steps != null) {
                            for (Step cs : steps) {
                                pipelineSteps.add(new PipelineStep(cs));
                            }
                        }
                    }
                }
            }
        }

        // add steps to pipeline
        pipelineManager = new PipelineManager(pipelineSteps);

    }
}
