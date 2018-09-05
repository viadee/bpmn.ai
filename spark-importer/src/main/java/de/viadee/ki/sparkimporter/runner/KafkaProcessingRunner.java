package de.viadee.ki.sparkimporter.runner;

import de.viadee.ki.sparkimporter.configuration.util.ConfigurationUtils;
import de.viadee.ki.sparkimporter.processing.PreprocessingRunner;
import de.viadee.ki.sparkimporter.processing.steps.dataprocessing.*;
import de.viadee.ki.sparkimporter.processing.steps.output.WriteToCSVStep;
import de.viadee.ki.sparkimporter.processing.steps.userconfig.*;
import de.viadee.ki.sparkimporter.runner.interfaces.ImportRunnerInterface;
import de.viadee.ki.sparkimporter.util.SparkImporterLogger;
import de.viadee.ki.sparkimporter.util.SparkImporterVariables;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static de.viadee.ki.sparkimporter.KafkaProcessingApplication.ARGS;

public class KafkaProcessingRunner implements ImportRunnerInterface {

    @Override
    public void run(SparkSession sparkSession) {

        SparkImporterLogger.getInstance().writeInfo("Starting data processing with data from: " + ARGS.getFileSource());

        final long startMillis = System.currentTimeMillis();

        //if there is no configuration file yet, write one in the next steps
        if(ConfigurationUtils.getInstance().getConfiguration() == null) {
            PreprocessingRunner.initialConfigToBeWritten = true;
            ConfigurationUtils.getInstance().createEmptyConfig();
        }

        //Load source parquet file
        Dataset<Row> dataset = sparkSession.read()
                .option("inferSchema", "true")
                .load(ARGS.getFileSource());

        //go through pipe elements
        // Define processing steps to run
        final PreprocessingRunner preprocessingRunner = new PreprocessingRunner();

        PreprocessingRunner.writeStepResultsIntoFile = ARGS.isWriteStepResultsToCSV();


        // add steps
        
        // user configuration step
        preprocessingRunner.addPreprocessorStep(new DataFilterStep());

        // user configuration step
        preprocessingRunner.addPreprocessorStep(new ColumnRemoveStep());

        //generic step
        preprocessingRunner.addPreprocessorStep(new ReduceColumnsDatasetStep());

        // user configuration step
        preprocessingRunner.addPreprocessorStep(new VariableFilterStep());

        // user configuration step
        preprocessingRunner.addPreprocessorStep(new VariableNameMappingStep());

        //generic steps
        preprocessingRunner.addPreprocessorStep(new DetermineVariableTypesStep());
        preprocessingRunner.addPreprocessorStep(new VariablesTypeEscalationStep());
        preprocessingRunner.addPreprocessorStep(new AggregateVariableUpdatesStep());
        preprocessingRunner.addPreprocessorStep(new AddVariablesColumnsStep());

        if(SparkImporterVariables.getDataLevel().equals("process")) {
            // process level
            preprocessingRunner.addPreprocessorStep(new AggregateProcessInstancesStep());
        } else {
            // activity level
            preprocessingRunner.addPreprocessorStep(new AggregateActivityInstancesStep());
            preprocessingRunner.addPreprocessorStep(new FillActivityInstancesHistoryStep());
        }

        preprocessingRunner.addPreprocessorStep(new AddReducedColumnsToDatasetStep());

        // user configuration step
        preprocessingRunner.addPreprocessorStep(new ColumnHashStep());

        //user configuration step
        preprocessingRunner.addPreprocessorStep(new TypeCastStep());

        //generic step
        preprocessingRunner.addPreprocessorStep(new WriteToCSVStep());

        // Run processing runner
        preprocessingRunner.run(dataset);

        //write initial config file
        if(PreprocessingRunner.initialConfigToBeWritten) {
            ConfigurationUtils.getInstance().writeConfigurationToFile();
        }

        final long endMillis = System.currentTimeMillis();
        SparkImporterLogger.getInstance().writeInfo("Kafka processing finished (took " + ((endMillis - startMillis) / 1000) + " seconds in total)");

        // Cleanup
        sparkSession.close();
    }
}
