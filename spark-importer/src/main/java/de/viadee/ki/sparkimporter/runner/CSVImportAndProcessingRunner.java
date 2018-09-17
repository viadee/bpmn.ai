package de.viadee.ki.sparkimporter.runner;

import de.viadee.ki.sparkimporter.configuration.util.ConfigurationUtils;
import de.viadee.ki.sparkimporter.processing.PreprocessingRunner;
import de.viadee.ki.sparkimporter.processing.steps.dataprocessing.*;
import de.viadee.ki.sparkimporter.processing.steps.importing.InitialCleanupStep;
import de.viadee.ki.sparkimporter.processing.steps.output.WriteToCSVStep;
import de.viadee.ki.sparkimporter.processing.steps.userconfig.*;
import de.viadee.ki.sparkimporter.runner.interfaces.ImportRunnerInterface;
import de.viadee.ki.sparkimporter.util.SparkImporterCSVArguments;
import de.viadee.ki.sparkimporter.util.SparkImporterLogger;
import de.viadee.ki.sparkimporter.util.SparkImporterUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static de.viadee.ki.sparkimporter.CSVImportAndProcessingApplication.ARGS;

public class CSVImportAndProcessingRunner implements ImportRunnerInterface {

    @Override
    public void run(SparkSession sparkSession) {

        final long startMillis = System.currentTimeMillis();

        //if there is no configuration file yet, write one in the next steps
        if(ConfigurationUtils.getInstance().getConfiguration() == null) {
            PreprocessingRunner.initialConfigToBeWritten = true;
            ConfigurationUtils.getInstance().createEmptyConfig();
        }

        SparkImporterLogger.getInstance().writeInfo("Starting CSV import and processing");
        SparkImporterLogger.getInstance().writeInfo("Importing CSV file: " + ARGS.getFileSource());

        //Load source CSV file
        Dataset<Row> dataset = sparkSession.read()
                .option("inferSchema", "true")
                .option("delimiter", ARGS.getDelimiter())
                .option("header", "true")
                .option("ignoreLeadingWhiteSpace", "false")
                .option("ignoreTrailingWhiteSpace", "false")
                .csv(ARGS.getFileSource());

        // write imported CSV structure to file for debugging
        if (SparkImporterCSVArguments.getInstance().isWriteStepResultsToCSV()) {
            SparkImporterUtils.getInstance().writeDatasetToCSV(dataset, "import_result");
        }

        SparkImporterLogger.getInstance().writeInfo("Starting data processing");

        InitialCleanupStep initialCleanupStep = new InitialCleanupStep();
        dataset = initialCleanupStep.runPreprocessingStep(dataset, false, "process");


        // Define processing steps to run
        final PreprocessingRunner preprocessingRunner = new PreprocessingRunner();
        PreprocessingRunner.writeStepResultsIntoFile = ARGS.isWriteStepResultsToCSV();

        //add steps

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
        preprocessingRunner.addPreprocessorStep(new AggregateProcessInstancesStep());
        preprocessingRunner.addPreprocessorStep(new CreateColumnsFromJsonStep());
        preprocessingRunner.addPreprocessorStep(new JsonVariableFilterStep());
        preprocessingRunner.addPreprocessorStep(new AddReducedColumnsToDatasetStep());

        // user configuration step
        preprocessingRunner.addPreprocessorStep(new ColumnHashStep());
        preprocessingRunner.addPreprocessorStep(new AddGeodataStep());
        preprocessingRunner.addPreprocessorStep(new MatchBrandsStep());
        preprocessingRunner.addPreprocessorStep(new TypeCastStep());

        //generic step
        preprocessingRunner.addPreprocessorStep(new WriteToCSVStep());

        // Run processing runner
        preprocessingRunner.run(dataset, "process");

        // Cleanup
        sparkSession.close();

        //write initial config file
        if(PreprocessingRunner.initialConfigToBeWritten) {
            ConfigurationUtils.getInstance().writeConfigurationToFile();
        }

        final long endMillis = System.currentTimeMillis();

        SparkImporterLogger.getInstance().writeInfo("CSV import and processing finished (took " + ((endMillis - startMillis) / 1000) + " seconds in total)");
    }
}
