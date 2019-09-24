package de.viadee.bpmnai.core.runner;

import de.viadee.bpmnai.core.util.BpmnaiUtils;
import de.viadee.bpmnai.core.configuration.Configuration;
import de.viadee.bpmnai.core.configuration.modelprediction.ModelPredictionConfiguration;
import de.viadee.bpmnai.core.configuration.preprocessing.PipelineStepConfiguration;
import de.viadee.bpmnai.core.configuration.preprocessing.Step;
import de.viadee.bpmnai.core.configuration.util.ConfigurationUtils;
import de.viadee.bpmnai.core.exceptions.FaultyConfigurationException;
import de.viadee.bpmnai.core.processing.PreprocessingRunner;
import de.viadee.bpmnai.core.processing.aggregation.AllButEmptyStringAggregationFunction;
import de.viadee.bpmnai.core.processing.aggregation.ProcessStatesAggregationFunction;
import de.viadee.bpmnai.core.processing.steps.PipelineManager;
import de.viadee.bpmnai.core.processing.steps.PipelineStep;
import de.viadee.bpmnai.core.runner.config.SparkRunnerConfig;
import de.viadee.bpmnai.core.util.logging.BpmnaiLogger;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.spark_project.guava.primitives.Longs;

import java.util.ArrayList;
import java.util.List;

public abstract class SparkPredictionServiceRunner {

    private static final Logger LOG = LoggerFactory.getLogger(SparkPredictionServiceRunner.class);

    private PipelineManager pipelineManager = null;
    protected SparkSession sparkSession = null;
    protected SparkRunnerConfig sparkRunnerConfig;

    private List<PipelineStep> pipelineSteps = new ArrayList<>();

    protected abstract void initialize();

    protected abstract List<PipelineStep> buildDefaultPipeline();

    private void checkConfig() {
        //if there is no configuration file yet, write one in the next steps
        if(ConfigurationUtils.getInstance().getConfiguration(true, this.sparkRunnerConfig) == null) {
            this.sparkRunnerConfig.setInitialConfigToBeWritten(true);
            ConfigurationUtils.getInstance().createEmptyConfig(this.sparkRunnerConfig);
        } else {
            BpmnaiLogger.getInstance().writeInfo("Configuration file found: " + this.sparkRunnerConfig.getWorkingDirectory() + "/" + ConfigurationUtils.getInstance().getConfigurationFileName(this.sparkRunnerConfig));
        }
    }

    private void writeConfig() {
        //write initial config file
        if(this.sparkRunnerConfig.isInitialConfigToBeWritten()) {
            ConfigurationUtils.getInstance().writeConfigurationToFile(this.sparkRunnerConfig);
        }
    }

    protected void registerUDFs() {
        // register our own aggregation function
        sparkSession.udf().register("AllButEmptyString", new AllButEmptyStringAggregationFunction());
        sparkSession.udf().register("ProcessState", new ProcessStatesAggregationFunction());
        sparkSession.udf().register("isALong", new UDF1<Object, Boolean>() {
            @Override
            public Boolean call(Object o) throws Exception {
                if(o instanceof Long)
                    return true;
                if(o instanceof String && Longs.tryParse((String) o) != null)
                    return true;
                return false;
            }
        }, DataTypes.BooleanType);
        sparkSession.udf().register("timestampStringToLong", new UDF1<Object, Long>() {
            @Override
            public Long call(Object o) throws Exception {
                if(o instanceof String && Longs.tryParse((String) o) != null) {
                    return Longs.tryParse((String) o) / 1000;
                }
                return null;
            }
        }, DataTypes.LongType);
    }

    public void setup(SparkRunnerConfig sparkRunnerConfig) throws FaultyConfigurationException {
        this.sparkRunnerConfig = sparkRunnerConfig;

        sparkSession = SparkSession.builder().getOrCreate();
        initialize();
        registerUDFs();
        checkConfig();
        configurePipelineSteps();
    }

    public Dataset<Row> run(Dataset dataset) {

        //only use configured variables for pipeline
        Configuration configuration = ConfigurationUtils.getInstance().getConfiguration(this.sparkRunnerConfig);
        List<String> predictionVars = configuration.getModelPredictionConfiguration().getPredictionVariables();
        List<Column> usedColumns = new ArrayList<>();
        for(String var : predictionVars) {
            usedColumns.add(new Column(var));
        }
        dataset = dataset.select(BpmnaiUtils.getInstance().asSeq(usedColumns));

        //go through pipe elements
        // Define processing steps to run
        final PreprocessingRunner preprocessingRunner = new PreprocessingRunner();

        for(PipelineStep ps : pipelineManager.getOrderedPipeline()) {
            preprocessingRunner.addPreprocessorStep(ps);
        }

        // Run processing runner
        Dataset<Row> resultDataset = preprocessingRunner.run(dataset, this.sparkRunnerConfig);

        writeConfig();

        return resultDataset;
    }

    public void configurePipelineSteps() throws FaultyConfigurationException {

        List<Step> steps = null;

        Configuration configuration = ConfigurationUtils.getInstance().getConfiguration(this.sparkRunnerConfig);

        if(this.sparkRunnerConfig.isInitialConfigToBeWritten()) {
            pipelineSteps = buildDefaultPipeline();

            ModelPredictionConfiguration modelPredictionConfiguration = configuration.getModelPredictionConfiguration();
            PipelineStepConfiguration pipelineStepConfiguration = modelPredictionConfiguration.getPipelineStepConfiguration();

            List<Step> configSteps = new ArrayList<>();
            for(PipelineStep ps : pipelineSteps) {
                Step s = new Step();
                s.setClassName(ps.getClassName());
                s.setDependsOn(ps.getDependsOn());
                s.setId(ps.getId());
                s.setParameters(ps.getStepParameters());
                s.setComment("");
                s.setActive(true);
                configSteps.add(s);
            }

            pipelineStepConfiguration.setSteps(configSteps);
        } else {
            if (configuration != null) {
                ModelPredictionConfiguration modelPredictionConfiguration = configuration.getModelPredictionConfiguration();
                if (modelPredictionConfiguration != null) {
                    PipelineStepConfiguration pipelineStepConfiguration = modelPredictionConfiguration.getPipelineStepConfiguration();
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
