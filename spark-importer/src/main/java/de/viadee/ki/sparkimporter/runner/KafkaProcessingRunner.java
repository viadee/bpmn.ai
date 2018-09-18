package de.viadee.ki.sparkimporter.runner;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;
import de.viadee.ki.sparkimporter.processing.PreprocessingRunner;
import de.viadee.ki.sparkimporter.processing.aggregation.AllButEmptyStringAggregationFunction;
import de.viadee.ki.sparkimporter.processing.aggregation.ProcessStatesAggregationFunction;
import de.viadee.ki.sparkimporter.processing.steps.PipelineStep;
import de.viadee.ki.sparkimporter.processing.steps.dataprocessing.*;
import de.viadee.ki.sparkimporter.processing.steps.output.WriteToCSVStep;
import de.viadee.ki.sparkimporter.processing.steps.userconfig.*;
import de.viadee.ki.sparkimporter.util.SparkImporterKafkaDataProcessingArguments;
import de.viadee.ki.sparkimporter.util.SparkImporterLogger;
import de.viadee.ki.sparkimporter.util.SparkImporterUtils;
import de.viadee.ki.sparkimporter.util.SparkImporterVariables;
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
    public static SparkImporterKafkaDataProcessingArguments ARGS;

    @Override
    public void initialize(String[] arguments) {
        ARGS = SparkImporterKafkaDataProcessingArguments.getInstance();

        // instantiate JCommander
        // Use JCommander for flexible usage of Parameters
        final JCommander jCommander = JCommander.newBuilder().addObject(SparkImporterKafkaDataProcessingArguments.getInstance()).build();
        try {
            jCommander.parse(arguments);
        } catch (final ParameterException e) {
            LOG.error("Parsing of parameters failed. Error message: " + e.getMessage());
            jCommander.usage();
            System.exit(1);
        }

        //workaround to overcome the issue that different Application argument classes are used but we need the target folder for the result steps
        SparkImporterVariables.setTargetFolder(ARGS.getFileDestination());
        SparkImporterVariables.setDevTypeCastCheckEnabled(ARGS.isDevTypeCastCheckEnabled());
        SparkImporterVariables.setRevCountEnabled(ARGS.isRevisionCount());
        SparkImporterUtils.setWorkingDirectory(ARGS.getWorkingDirectory());
        SparkImporterLogger.setLogDirectory(ARGS.getLogDirectory());

        dataLevel = ARGS.getDataLavel();

        PreprocessingRunner.writeStepResultsIntoFile = ARGS.isWriteStepResultsToCSV();

        // Delete destination files, required to avoid exception during runtime
        FileUtils.deleteQuietly(new File(ARGS.getFileDestination()));

        // register our own aggregation function
        sparkSession.udf().register("AllButEmptyString", new AllButEmptyStringAggregationFunction());
        sparkSession.udf().register("ProcessState", new ProcessStatesAggregationFunction());

        SparkImporterLogger.getInstance().writeInfo("Starting data processing with data from: " + ARGS.getFileSource());
    }

    @Override
    protected List<PipelineStep> buildDefaultPipeline() {
        List<PipelineStep> pipelineSteps = new ArrayList<>();

        pipelineSteps.add(new PipelineStep(new DataFilterStep(), ""));
        pipelineSteps.add(new PipelineStep(new ColumnRemoveStep(), "DataFilterStep"));
        pipelineSteps.add(new PipelineStep(new ReduceColumnsDatasetStep(), "ColumnRemoveStep"));
        pipelineSteps.add(new PipelineStep(new VariableFilterStep(), "ReduceColumnsDatasetStep"));
        pipelineSteps.add(new PipelineStep(new VariableNameMappingStep(), "VariableFilterStep"));
        pipelineSteps.add(new PipelineStep(new DetermineVariableTypesStep(), "VariableNameMappingStep"));
        pipelineSteps.add(new PipelineStep(new VariablesTypeEscalationStep(), "DetermineVariableTypesStep"));
        pipelineSteps.add(new PipelineStep(new AggregateVariableUpdatesStep(), "VariablesTypeEscalationStep"));
        pipelineSteps.add(new PipelineStep(new AddVariablesColumnsStep(), "AggregateVariableUpdatesStep"));

        if(dataLevel.equals("process")) {
            // process level
            pipelineSteps.add(new PipelineStep(new AggregateProcessInstancesStep(), "AddVariablesColumnsStep"));
        } else {
            // activity level
            pipelineSteps.add(new PipelineStep(new AggregateActivityInstancesStep(), "AddVariablesColumnsStep"));
        }

        pipelineSteps.add(new PipelineStep(new CreateColumnsFromJsonStep(), dataLevel.equals("process") ? "AggregateProcessInstancesStep" : "AggregateActivityInstancesStep"));
        pipelineSteps.add(new PipelineStep(new JsonVariableFilterStep(), "CreateColumnsFromJsonStep"));

        if(dataLevel.equals("activity")) {
            // activity level
            pipelineSteps.add(new PipelineStep(new FillActivityInstancesHistoryStep(), "JsonVariableFilterStep"));
        }

        pipelineSteps.add(new PipelineStep(new AddReducedColumnsToDatasetStep(), dataLevel.equals("process") ? "JsonVariableFilterStep" : "FillActivityInstancesHistoryStep"));
        pipelineSteps.add(new PipelineStep(new ColumnHashStep(), "AddReducedColumnsToDatasetStep"));
        pipelineSteps.add(new PipelineStep(new TypeCastStep(), "ColumnHashStep"));
        pipelineSteps.add(new PipelineStep(new WriteToCSVStep(), "TypeCastStep"));

        return pipelineSteps;
    }

    @Override
    protected Dataset<Row> loadInitialDataset() {
        //Load source parquet file
        Dataset<Row> dataset = sparkSession.read()
                .option("inferSchema", "true")
                .load(ARGS.getFileSource());

        return dataset;
    }
}
