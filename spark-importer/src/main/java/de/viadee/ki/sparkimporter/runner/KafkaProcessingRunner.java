package de.viadee.ki.sparkimporter.runner;

import de.viadee.ki.sparkimporter.processing.PreprocessingRunner;
import de.viadee.ki.sparkimporter.processing.steps.dataprocessing.*;
import de.viadee.ki.sparkimporter.processing.steps.output.DataSinkFilterStep;
import de.viadee.ki.sparkimporter.processing.steps.output.WriteToCSVStep;
import de.viadee.ki.sparkimporter.processing.steps.userconfig.FilterVariablesStep;
import de.viadee.ki.sparkimporter.runner.interfaces.ImportRunnerInterface;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static de.viadee.ki.sparkimporter.KafkaProcessingApplication.ARGS;

public class KafkaProcessingRunner implements ImportRunnerInterface {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaProcessingRunner.class);

    @Override
    public void run(SparkSession sparkSession) {

        //Load source parquet file
        Dataset<Row> dataset = sparkSession.read()
                .option("inferSchema", "true")
                .load(ARGS.getFileSource());

        LOG.info("================ STARTING PROCESSING DATA ================");

        //go through pipe elements
        // Define processing steps to run
        final PreprocessingRunner preprocessingRunner = new PreprocessingRunner();

        PreprocessingRunner.writeStepResultsIntoFile = ARGS.isWriteStepResultsToCSV();

        // it's faster if we do not reduce the dataset columns in the beginning and
        // rejoin the dataset later, left steps in commented if required later
        preprocessingRunner.addPreprocessorStep(new DataSinkFilterStep());
        preprocessingRunner.addPreprocessorStep(new ReduceColumnsDatasetStep());

        // user configuration step
        preprocessingRunner.addPreprocessorStep(new FilterVariablesStep());

        preprocessingRunner.addPreprocessorStep(new GetVariablesTypesOccurenceStep());
        preprocessingRunner.addPreprocessorStep(new VariablesTypeEscalationStep());
        preprocessingRunner.addPreprocessorStep(new AggregateVariableUpdatesStep());
        preprocessingRunner.addPreprocessorStep(new AddVariablesColumnsStep());
        preprocessingRunner.addPreprocessorStep(new AggregateProcessInstancesStep());
        preprocessingRunner.addPreprocessorStep(new AddRemovedColumnsToDatasetStep());
        preprocessingRunner.addPreprocessorStep(new WriteToCSVStep());

        // Run processing runner
        preprocessingRunner.run(dataset);

        // Cleanup
        sparkSession.close();
    }
}
