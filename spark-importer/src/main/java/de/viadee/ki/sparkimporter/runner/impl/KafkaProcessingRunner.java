package de.viadee.ki.sparkimporter.runner.impl;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;
import de.viadee.ki.sparkimporter.processing.steps.PipelineStep;
import de.viadee.ki.sparkimporter.processing.steps.dataprocessing.*;
import de.viadee.ki.sparkimporter.processing.steps.output.WriteToDiscStep;
import de.viadee.ki.sparkimporter.runner.SparkRunner;
import de.viadee.ki.sparkimporter.runner.config.SparkRunnerConfig;
import de.viadee.ki.sparkimporter.util.SparkImporterVariables;
import de.viadee.ki.sparkimporter.util.arguments.KafkaProcessingArguments;
import de.viadee.ki.sparkimporter.util.logging.SparkImporterLogger;
import org.apache.commons.io.FileUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class KafkaProcessingRunner extends SparkRunner {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaProcessingRunner.class);

    public KafkaProcessingRunner() { super(); }

    public KafkaProcessingRunner(SparkRunnerConfig config) {
        super(config);
    }

    @Override
    protected void initialize(String[] arguments) {
        KafkaProcessingArguments kafkaProcessingArguments = KafkaProcessingArguments.getInstance();

        // instantiate JCommander
        // Use JCommander for flexible usage of Parameters
        final JCommander jCommander = JCommander.newBuilder().addObject(KafkaProcessingArguments.getInstance()).build();
        try {
            jCommander.parse(arguments);
        } catch (final ParameterException e) {
            LOG.error("Parsing of parameters failed. Error message: " + e.getMessage());
            jCommander.usage();
            System.exit(1);
        }

        //parse arguments to create SparkRunnerConfig
        kafkaProcessingArguments.createOrUpdateSparkRunnerConfig(this.sparkRunnerConfig);

        // Delete destination files, required to avoid exception during runtime
        FileUtils.deleteQuietly(new File(this.sparkRunnerConfig.getTargetFolder()));

        SparkImporterLogger.getInstance().writeInfo("Starting data processing with data from: " + this.sparkRunnerConfig.getSourceFolder());
    }

    @Override
    protected List<PipelineStep> buildDefaultPipeline() {
        List<PipelineStep> pipelineSteps = new ArrayList<>();

        pipelineSteps.add(new PipelineStep(new DataFilterStep(), ""));
        pipelineSteps.add(new PipelineStep(new ColumnRemoveStep(), "DataFilterStep"));
        pipelineSteps.add(new PipelineStep(new ReduceColumnsStep(), "ColumnRemoveStep"));
        pipelineSteps.add(new PipelineStep(new DetermineProcessVariablesStep(), "ReduceColumnsStep"));
        pipelineSteps.add(new PipelineStep(new AddVariableColumnsStep(), "DetermineProcessVariablesStep"));

        if(sparkRunnerConfig.getDataLevel().equals(SparkImporterVariables.DATA_LEVEL_PROCESS)) {
            // process level
            pipelineSteps.add(new PipelineStep(new AggregateProcessInstancesStep(), "AddVariableColumnsStep"));
        } else {
            // activity level
            pipelineSteps.add(new PipelineStep(new AggregateActivityInstancesStep(), "AddVariableColumnsStep"));
        }

       // pipelineSteps.add(new PipelineStep(new DataFilterOnActivityStep(), "AddVariablesColumnsStep"));

        pipelineSteps.add(new PipelineStep(new CreateColumnsFromJsonStep(), sparkRunnerConfig.getDataLevel().equals(SparkImporterVariables.DATA_LEVEL_PROCESS) ? "AggregateProcessInstancesStep" : "AggregateActivityInstancesStep"));

        if(sparkRunnerConfig.getDataLevel().equals(SparkImporterVariables.DATA_LEVEL_ACTIVITY)) {
            // activity level
            pipelineSteps.add(new PipelineStep(new FillActivityInstancesHistoryStep(), "CreateColumnsFromJsonStep"));
        }

        pipelineSteps.add(new PipelineStep(new AddReducedColumnsToDatasetStep(), sparkRunnerConfig.getDataLevel().equals(SparkImporterVariables.DATA_LEVEL_PROCESS) ? "CreateColumnsFromJsonStep" : "FillActivityInstancesHistoryStep"));
        pipelineSteps.add(new PipelineStep(new ColumnHashStep(), "AddReducedColumnsToDatasetStep"));
        pipelineSteps.add(new PipelineStep(new TypeCastStep(), "ColumnHashStep"));
        pipelineSteps.add(new PipelineStep(new WriteToDiscStep(), "TypeCastStep"));

        return pipelineSteps;
    }

    @Override
    protected Dataset<Row> loadInitialDataset() {
        //Load source parquet file
        Dataset<Row> dataset = sparkSession.read()
                .option("inferSchema", "true")
                .load(this.sparkRunnerConfig.getSourceFolder());

        return dataset;
    }
}
