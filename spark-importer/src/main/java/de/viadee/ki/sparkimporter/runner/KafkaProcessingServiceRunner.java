package de.viadee.ki.sparkimporter.runner;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;
import de.viadee.ki.sparkimporter.processing.PreprocessingRunner;
import de.viadee.ki.sparkimporter.processing.aggregation.AllButEmptyStringAggregationFunction;
import de.viadee.ki.sparkimporter.processing.aggregation.ProcessStatesAggregationFunction;
import de.viadee.ki.sparkimporter.processing.steps.PipelineStep;
import de.viadee.ki.sparkimporter.processing.steps.dataprocessing.*;
import de.viadee.ki.sparkimporter.processing.steps.output.WriteToDiscStep;
import de.viadee.ki.sparkimporter.processing.steps.userconfig.*;
import de.viadee.ki.sparkimporter.util.SparkImporterKafkaDataProcessingArguments;
import de.viadee.ki.sparkimporter.util.SparkImporterLogger;
import de.viadee.ki.sparkimporter.util.SparkImporterUtils;
import de.viadee.ki.sparkimporter.util.SparkImporterVariables;
import org.apache.commons.io.FileUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class KafkaProcessingServiceRunner extends SparkServiceRunner {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaProcessingServiceRunner.class);

    @Override
    protected void initialize() {
        //workaround to overcome the issue that different Application argument classes are used but we need the target folder for the result steps
        SparkImporterUtils.setWorkingDirectory("/Users/mim/Desktop/provi_service_config ");
        SparkImporterLogger.setLogDirectory("/Users/mim/Desktop/provi_service_config ");



        //SparkImporterLogger.getInstance().writeInfo("Starting data processing with data from: " + ARGS.getFileSource());
    }

    @Override
    protected List<PipelineStep> buildDefaultPipeline() {
        List<PipelineStep> pipelineSteps = new ArrayList<>();

        pipelineSteps.add(new PipelineStep(new CreateColumnsFromJsonStep(), ""));
        pipelineSteps.add(new PipelineStep(new ColumnHashStep(), "CreateColumnsFromJsonStep"));
        pipelineSteps.add(new PipelineStep(new TypeCastStep(), "ColumnHashStep"));

        return pipelineSteps;
    }
}
