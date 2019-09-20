package de.viadee.ki.sparkimporter.runner;

import de.viadee.ki.sparkimporter.configuration.Configuration;
import de.viadee.ki.sparkimporter.configuration.preprocessing.PipelineStepConfiguration;
import de.viadee.ki.sparkimporter.configuration.preprocessing.PreprocessingConfiguration;
import de.viadee.ki.sparkimporter.configuration.preprocessing.Step;
import de.viadee.ki.sparkimporter.configuration.util.ConfigurationUtils;
import de.viadee.ki.sparkimporter.exceptions.FaultyConfigurationException;
import de.viadee.ki.sparkimporter.listener.SparkRunnerListener;
import de.viadee.ki.sparkimporter.processing.PreprocessingRunner;
import de.viadee.ki.sparkimporter.processing.aggregation.AllButEmptyStringAggregationFunction;
import de.viadee.ki.sparkimporter.processing.aggregation.ProcessStatesAggregationFunction;
import de.viadee.ki.sparkimporter.processing.steps.PipelineManager;
import de.viadee.ki.sparkimporter.processing.steps.PipelineStep;
import de.viadee.ki.sparkimporter.processing.steps.dataprocessing.AddVariableColumnsStep;
import de.viadee.ki.sparkimporter.processing.steps.dataprocessing.DetermineProcessVariablesStep;
import de.viadee.ki.sparkimporter.processing.steps.dataprocessing.ReduceColumnsStep;
import de.viadee.ki.sparkimporter.runner.config.SparkRunnerConfig;
import de.viadee.ki.sparkimporter.util.SparkImporterVariables;
import de.viadee.ki.sparkimporter.util.helper.SparkBroadcastHelper;
import de.viadee.ki.sparkimporter.util.logging.SparkImporterLogger;
import org.apache.spark.scheduler.*;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.types.DataTypes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.spark_project.guava.primitives.Longs;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public abstract class SparkRunner {

    private static final Logger LOG = LoggerFactory.getLogger(SparkRunner.class);

    private PipelineManager pipelineManager = null;
    protected SparkSession sparkSession = null;

    private Dataset<Row> dataset;
    private List<PipelineStep> pipelineSteps = new ArrayList<>();

    protected SparkRunnerConfig sparkRunnerConfig = new SparkRunnerConfig();

    protected SparkRunner() {}

    protected SparkRunner(SparkRunnerConfig config) {
        this.sparkRunnerConfig = config;
    }

    protected abstract void initialize(String[] arguments);

    protected abstract List<PipelineStep> buildDefaultPipeline();

    protected List<PipelineStep> buildMinimalPipeline(){
        List<PipelineStep> pipelineSteps = new ArrayList<>();

        pipelineSteps.add(new PipelineStep(new ReduceColumnsStep(), ""));
        pipelineSteps.add(new PipelineStep(new DetermineProcessVariablesStep(), "ReduceColumnsStep"));
        pipelineSteps.add(new PipelineStep(new AddVariableColumnsStep(), "DetermineProcessVariablesStep"));
        //pipelineSteps.add(new PipelineStep(new CreateColumnsFromJsonStep(), "AddVariableColumnsStep"));

        return pipelineSteps;
    }

    protected abstract Dataset<Row> loadInitialDataset();

    public enum RUNNING_MODE {
        CSV_IMPORT_AND_PROCESSING("csv"),
        KAFKA_IMPORT("kafka_import"),
        KAFKA_PROCESSING("kafka_process");

        private String runnerMode;

        RUNNING_MODE(String runnerMode) {
            this.runnerMode = runnerMode;
        }

        public String getModeString() {
            return runnerMode;
        }
    }

    private void checkConfig() {
        //if there is no configuration file yet or an completely empty one, write one in the next steps
        if(ConfigurationUtils.getInstance().getConfiguration(true, this.sparkRunnerConfig) == null
                || ConfigurationUtils.getInstance().getConfiguration(true, this.sparkRunnerConfig).isEmpty()) {
            if(!this.sparkRunnerConfig.getRunningMode().equals(RUNNING_MODE.KAFKA_IMPORT)) {
                this.sparkRunnerConfig.setMinimalPipelineToBeBuild(true);
            }
            this.sparkRunnerConfig.setInitialConfigToBeWritten(true);
            ConfigurationUtils.getInstance().createEmptyConfig(this.sparkRunnerConfig);
        } else {
            SparkImporterLogger.getInstance().writeInfo("Configuration file found: " + this.sparkRunnerConfig.getWorkingDirectory() + "/" + ConfigurationUtils.getInstance().getConfigurationFileName(this.sparkRunnerConfig));
            ConfigurationUtils.getInstance().validateConfigurationFileVsSparkRunnerConfig(this.sparkRunnerConfig);
        }
    }

    private void writeConfig() {
        //write initial config file
        if(this.sparkRunnerConfig.isInitialConfigToBeWritten()) {
            Configuration configuration = ConfigurationUtils.getInstance().getConfiguration(this.sparkRunnerConfig);
            configuration.getPreprocessingConfiguration().setDataLevel(this.sparkRunnerConfig.getDataLevel());
            ConfigurationUtils.getInstance().writeConfigurationToFile(this.sparkRunnerConfig);
        }
    }

    private void registerUDFs() {
        // register our own aggregation function
        sparkSession.udf().register("AllButEmptyString", new AllButEmptyStringAggregationFunction());
        sparkSession.udf().register("ProcessState", new ProcessStatesAggregationFunction());
        sparkSession.udf().register("isALong", (UDF1<Object, Boolean>) o -> {
            if(o instanceof Long)
                return true;
            if(o instanceof String && Longs.tryParse((String) o) != null)
                return true;
            return false;
        }, DataTypes.BooleanType);
        sparkSession.udf().register("timestampStringToLong", (UDF1<Object, Long>) o -> {
            if(o instanceof String && Longs.tryParse((String) o) != null) {
                return Longs.tryParse((String) o) / 1000;
            }
            return null;
        }, DataTypes.LongType);
        sparkSession.udf().register("activityBeforeTimestamp", (UDF2<String, String, String>) (s, s2) -> {
            // get broadcast
            Map<String, String> activities = (Map<String, String>) SparkBroadcastHelper.getInstance().getBroadcastVariable(SparkBroadcastHelper.BROADCAST_VARIABLE.PROCESS_INSTANCE_TIMESTAMP_MAP);
            // is pid contained in broadcast?
            if (activities == null || activities.isEmpty()){
                return "Error: Broadcast not found";
            } else {
                if (activities.containsKey(s)) {
                    Timestamp tsAct = new Timestamp(Long.parseLong(activities.get(s)));
                    if(s2 == null || s2.isEmpty()){
                        return "FALSE";
                    }
                    Timestamp tsObject = new Timestamp(Long.parseLong(s2));
                    if (tsObject.after(tsAct)) {
                        return "FALSE";
                    } else {
                        return "TRUE";
                    }
                }
            }
            return "FALSE";
        }, DataTypes.StringType);
    }

    public void run(String[] arguments) throws FaultyConfigurationException {
        run(arguments, null);
    }

    public void run(String[] arguments, SparkRunnerListener sparkRunnerListener) throws FaultyConfigurationException {
        // spark configuration is being loaded from Environment (e.g. when using spark-submit)
        sparkSession = SparkSession.builder().getOrCreate();

        // listen for application progress and write to console
        LOG.info("Spark application '" + sparkSession.sparkContext().appName() + "' (ID: " + sparkSession.sparkContext().applicationId() + ") started.");

        sparkSession.sparkContext().addSparkListener(new SparkListener() {
            @Override
            public void onJobEnd(SparkListenerJobEnd jobEnd) {
                super.onJobEnd(jobEnd);

                LOG.info("... job " + jobEnd.jobId() + " finished.");
            }

            @Override
            public void onJobStart(SparkListenerJobStart jobStart) {
                super.onJobStart(jobStart);

                LOG.info("Spark job " + jobStart.jobId() + " started (has " + jobStart.stageIds().size() + " " + (jobStart.stageIds().size() == 1 ? "stage" : "stages") + ") ...");
            }

            @Override
            public void onApplicationEnd(SparkListenerApplicationEnd applicationEnd) {
                super.onApplicationEnd(applicationEnd);

                LOG.info("Spark application finished.");
            }
        });

        AtomicInteger tasksTotal = new AtomicInteger();
        AtomicInteger tasksDone = new AtomicInteger();

        SparkListener sparkListener = new SparkListener() {
            @Override
            public void onStageSubmitted(SparkListenerStageSubmitted stageSubmitted) {
                super.onStageSubmitted(stageSubmitted);

                tasksTotal.getAndAdd(stageSubmitted.stageInfo().numTasks());
                sparkRunnerListener.onProgressUpdate(null, tasksDone.get(), tasksTotal.get());
            }

            @Override
            public void onStageCompleted(SparkListenerStageCompleted stageCompleted) {
                super.onStageCompleted(stageCompleted);

                sparkRunnerListener.onProgressUpdate(null, tasksDone.get(), tasksTotal.get());
                tasksDone.getAndAdd(stageCompleted.stageInfo().numTasks());
            }
        };

        if(sparkRunnerListener != null) {
            sparkSession.sparkContext().addSparkListener(sparkListener);
        }

        registerUDFs();
        initialize(arguments);
        checkConfig();
        configurePipelineSteps();
        dataset = loadInitialDataset();
        
        // filter dataset if only a specific processDefinitionId should be preprocessed (-pf)
        if(this.sparkRunnerConfig.getProcessFilterDefinitionId() != null) {
        	dataset = dataset.filter(dataset.col(SparkImporterVariables.VAR_PROCESS_DEF_ID).equalTo(this.sparkRunnerConfig.getProcessFilterDefinitionId()));
        }
        
        //go through pipe elements
        // Define processing steps to run
        final PreprocessingRunner preprocessingRunner = new PreprocessingRunner();

        for(PipelineStep ps : pipelineManager.getOrderedPipeline()) {
            preprocessingRunner.addPreprocessorStep(ps);
        }

        final long startMillis = System.currentTimeMillis();

        // Run processing runner
        preprocessingRunner.run(dataset, this.sparkRunnerConfig);

        final long endMillis = System.currentTimeMillis();

        String logMessage = "Job ran for " + ((endMillis - startMillis) / 1000) + " seconds in total";
        LOG.info(logMessage);
        SparkImporterLogger.getInstance().writeInfo(logMessage);

        /**
         * if the created configuration file is a minimal one, overwrite the steps with the default pipeline
         */
        if (this.sparkRunnerConfig.isMinimalPipelineToBeBuild()){
            logMessage = "Filling the minimal configuration pipeline with the applications default pipeline...";
            LOG.info(logMessage);
            SparkImporterLogger.getInstance().writeInfo(logMessage);
            logMessage = "Execute again to process data with under the newly created configuration.";
            LOG.info(logMessage);
            SparkImporterLogger.getInstance().writeInfo(logMessage);
            overwritePipelineSteps();
        }

        // Cleanup
        if(this.sparkRunnerConfig.isCloseSparkSessionAfterRun()) {
            sparkSession.close();
        }

        writeConfig();

        if(sparkRunnerListener != null) {
            sparkRunnerListener.onFinished(true);
            sparkSession.sparkContext().removeSparkListener(sparkListener);
        }
    }

    public void overwritePipelineSteps() {
        Configuration configuration = ConfigurationUtils.getInstance().getConfiguration(this.sparkRunnerConfig);

        if (this.sparkRunnerConfig.isInitialConfigToBeWritten()) {
            pipelineSteps = buildDefaultPipeline();

            PreprocessingConfiguration preprocessingConfiguration = configuration.getPreprocessingConfiguration();
            PipelineStepConfiguration pipelineStepConfiguration = preprocessingConfiguration.getPipelineStepConfiguration();

            List<Step> configSteps = new ArrayList<>();
            for (PipelineStep ps : pipelineSteps) {
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
        }
    }

    public void configurePipelineSteps() throws FaultyConfigurationException {

        List<Step> steps = null;

        Configuration configuration = ConfigurationUtils.getInstance().getConfiguration(this.sparkRunnerConfig);

        if(this.sparkRunnerConfig.isInitialConfigToBeWritten()) {
            if(!this.sparkRunnerConfig.getRunningMode().equals(RUNNING_MODE.KAFKA_IMPORT)) {
                pipelineSteps = buildMinimalPipeline();
            } else {
                pipelineSteps = buildDefaultPipeline();
            }

            PreprocessingConfiguration preprocessingConfiguration = configuration.getPreprocessingConfiguration();
            PipelineStepConfiguration pipelineStepConfiguration = preprocessingConfiguration.getPipelineStepConfiguration();

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
                PreprocessingConfiguration preprocessingConfiguration = configuration.getPreprocessingConfiguration();
                if (preprocessingConfiguration != null) {
                    PipelineStepConfiguration pipelineStepConfiguration = preprocessingConfiguration.getPipelineStepConfiguration();
                    if (pipelineStepConfiguration != null) {
                        steps = pipelineStepConfiguration.getSteps();

                        if (steps != null) {
                            for (Step cs : steps) {
                            	if(cs.getActive() != false) {
                            		pipelineSteps.add(new PipelineStep(cs));
                            	}                               
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
