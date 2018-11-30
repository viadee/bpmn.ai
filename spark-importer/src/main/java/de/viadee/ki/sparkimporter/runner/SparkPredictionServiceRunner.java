package de.viadee.ki.sparkimporter.runner;

import de.viadee.ki.sparkimporter.configuration.Configuration;
import de.viadee.ki.sparkimporter.configuration.modelprediction.ModelPredictionConfiguration;
import de.viadee.ki.sparkimporter.configuration.preprocessing.PipelineStepConfiguration;
import de.viadee.ki.sparkimporter.configuration.preprocessing.Step;
import de.viadee.ki.sparkimporter.configuration.util.ConfigurationUtils;
import de.viadee.ki.sparkimporter.exceptions.FaultyConfigurationException;
import de.viadee.ki.sparkimporter.processing.PreprocessingRunner;
import de.viadee.ki.sparkimporter.processing.aggregation.AllButEmptyStringAggregationFunction;
import de.viadee.ki.sparkimporter.processing.aggregation.ProcessStatesAggregationFunction;
import de.viadee.ki.sparkimporter.processing.steps.PipelineManager;
import de.viadee.ki.sparkimporter.processing.steps.PipelineStep;
import de.viadee.ki.sparkimporter.util.SparkImporterUtils;
import de.viadee.ki.sparkimporter.util.SparkImporterVariables;
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

    private Dataset<Row> dataset;
    protected String dataLevel = SparkImporterVariables.DATA_LEVEL_PROCESS;
    private List<PipelineStep> pipelineSteps = new ArrayList<>();

    protected abstract void initialize();

    protected abstract List<PipelineStep> buildDefaultPipeline();

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

    public void setup() throws FaultyConfigurationException {
        sparkSession = SparkSession.builder().getOrCreate();
        initialize();
        registerUDFs();
        checkConfig();
        configurePipelineSteps();
    }

    public Dataset<Row> run(Dataset dataset) {

        //only use configured variables for pipeline
        Configuration configuration = ConfigurationUtils.getInstance().getConfiguration();
        List<String> predictionVars = configuration.getModelPredictionConfiguration().getPredictionVariables();
        List<Column> usedColumns = new ArrayList<>();
        for(String var : predictionVars) {
            usedColumns.add(new Column(var));
        }
        dataset = dataset.select(SparkImporterUtils.getInstance().asSeq(usedColumns));

        //go through pipe elements
        // Define processing steps to run
        final PreprocessingRunner preprocessingRunner = new PreprocessingRunner();

        for(PipelineStep ps : pipelineManager.getOrderedPipeline()) {
            preprocessingRunner.addPreprocessorStep(ps);
        }

        // Run processing runner
        Dataset<Row> resultDataset = preprocessingRunner.run(dataset, dataLevel);

        writeConfig();

        return resultDataset;
    }

    public void configurePipelineSteps() throws FaultyConfigurationException {

        List<Step> steps = null;

        Configuration configuration = ConfigurationUtils.getInstance().getConfiguration();

        if(PreprocessingRunner.initialConfigToBeWritten) {
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
