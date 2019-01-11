package de.viadee.ki.sparkimporter.runner;

import de.viadee.ki.sparkimporter.configuration.Configuration;
import de.viadee.ki.sparkimporter.configuration.preprocessing.PipelineStepConfiguration;
import de.viadee.ki.sparkimporter.configuration.preprocessing.PreprocessingConfiguration;
import de.viadee.ki.sparkimporter.configuration.preprocessing.Step;
import de.viadee.ki.sparkimporter.configuration.util.ConfigurationUtils;
import de.viadee.ki.sparkimporter.exceptions.FaultyConfigurationException;
import de.viadee.ki.sparkimporter.processing.PreprocessingRunner;
import de.viadee.ki.sparkimporter.processing.aggregation.AllButEmptyStringAggregationFunction;
import de.viadee.ki.sparkimporter.processing.aggregation.ProcessStatesAggregationFunction;
import de.viadee.ki.sparkimporter.processing.steps.PipelineManager;
import de.viadee.ki.sparkimporter.processing.steps.PipelineStep;
import de.viadee.ki.sparkimporter.processing.steps.dataprocessing.CreateColumnsFromJsonStep;
import de.viadee.ki.sparkimporter.processing.steps.dataprocessing.DetermineVariableTypesStep;
import de.viadee.ki.sparkimporter.processing.steps.dataprocessing.ReduceColumnsDatasetStep;
import de.viadee.ki.sparkimporter.processing.steps.dataprocessing.VariablesTypeEscalationStep;
import de.viadee.ki.sparkimporter.util.SparkImporterLogger;
import de.viadee.ki.sparkimporter.util.SparkImporterVariables;
import org.apache.spark.scheduler.SparkListener;
import org.apache.spark.scheduler.SparkListenerApplicationEnd;
import org.apache.spark.scheduler.SparkListenerJobEnd;
import org.apache.spark.scheduler.SparkListenerJobStart;
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

public abstract class SparkRunner {

    private static final Logger LOG = LoggerFactory.getLogger(SparkRunner.class);

    private PipelineManager pipelineManager = null;
    protected SparkSession sparkSession = null;

    private Dataset<Row> dataset;
    protected String dataLevel = SparkImporterVariables.DATA_LEVEL_PROCESS;
    private List<PipelineStep> pipelineSteps = new ArrayList<>();

    protected abstract void initialize(String[] arguments);

    protected abstract List<PipelineStep> buildDefaultPipeline();

    protected List<PipelineStep> buildMinimalPipeline(){
        List<PipelineStep> pipelineSteps = new ArrayList<>();

        pipelineSteps.add(new PipelineStep(new ReduceColumnsDatasetStep(), ""));
        pipelineSteps.add(new PipelineStep(new DetermineVariableTypesStep(), "ReduceColumnsDatasetStep"));
        pipelineSteps.add(new PipelineStep(new VariablesTypeEscalationStep(), "DetermineVariableTypesStep"));
        pipelineSteps.add(new PipelineStep(new CreateColumnsFromJsonStep(), "VariablesTypeEscalationStep"));

        return pipelineSteps;
    }

    protected abstract Dataset<Row> loadInitialDataset();

    private void checkConfig() {
        //if there is no configuration file yet or an completely empty one, write one in the next steps
        if(ConfigurationUtils.getInstance().getConfiguration(true) == null
                || ConfigurationUtils.getInstance().getConfiguration(true).isEmpty()) {
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
    }

    public void run(String[] arguments) throws FaultyConfigurationException {
        // spark configuration is being loaded from Environment (e.g. when using spark-submit)
        sparkSession = SparkSession.builder().getOrCreate();

        // listen for application progress and write to console
        System.out.println("Spark application '" + sparkSession.sparkContext().appName() + "' (ID: " + sparkSession.sparkContext().applicationId() + ") started.");
        sparkSession.sparkContext().addSparkListener(new SparkListener() {
            @Override
            public void onJobEnd(SparkListenerJobEnd jobEnd) {
                super.onJobEnd(jobEnd);

                System.out.println("... job " + jobEnd.jobId() + " finished.");
            }

            @Override
            public void onJobStart(SparkListenerJobStart jobStart) {
                super.onJobStart(jobStart);

                System.out.print("Spark job " + jobStart.jobId() + " started (has " + jobStart.stageIds().size() + " " + (jobStart.stageIds().size() == 1 ? "stage" : "stages") + ") ...");
            }

            @Override
            public void onApplicationEnd(SparkListenerApplicationEnd applicationEnd) {
                super.onApplicationEnd(applicationEnd);

                System.out.println("Spark application finished.");
            }
        });

        registerUDFs();
        initialize(arguments);
        checkConfig();
        configurePipelineSteps();
        dataset = loadInitialDataset();
        
        // filter dataset if only a specific processDefinitionId should be preprocessed (-pf)
        if(SparkImporterVariables.getProcessFilterDefinitionId() != null) {
        	dataset = dataset.filter(dataset.col(SparkImporterVariables.VAR_PROCESS_DEF_ID).equalTo(SparkImporterVariables.getProcessFilterDefinitionId()));
        }
        
        // TODO 
        /* transform all column names to lower case       
        for(String col : dataset.columns()) {
        	dataset = dataset.withColumnRenamed(col, col.toLowerCase());
        }*/
      
        
        
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

        /**
         * if the created configuration file is a minimal one, overwrite the steps with the default pipeline
         */
        if (PreprocessingRunner.initialConfigToBeWritten){
            logMessage = "Filling the minimal configuration pipeline with the applications default pipeline...";
            LOG.info(logMessage);
            SparkImporterLogger.getInstance().writeInfo(logMessage);
            logMessage = "Execute again to process data with under the newly created configuration.";
            LOG.info(logMessage);
            SparkImporterLogger.getInstance().writeInfo(logMessage);
            overwritePipelineSteps();
        }

        // Cleanup
        sparkSession.close();

        writeConfig();
    }

    public void overwritePipelineSteps() throws FaultyConfigurationException {
        List<Step> steps = null;

        Configuration configuration = ConfigurationUtils.getInstance().getConfiguration();

        if (PreprocessingRunner.initialConfigToBeWritten) {
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

        Configuration configuration = ConfigurationUtils.getInstance().getConfiguration();

        if(PreprocessingRunner.initialConfigToBeWritten) {
            pipelineSteps = buildMinimalPipeline();

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
