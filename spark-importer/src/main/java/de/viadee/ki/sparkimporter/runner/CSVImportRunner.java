package de.viadee.ki.sparkimporter.runner;

import de.viadee.ki.sparkimporter.configuration.Configuration;
import de.viadee.ki.sparkimporter.preprocessing.PreprocessingRunner;
import de.viadee.ki.sparkimporter.preprocessing.steps.*;
import de.viadee.ki.sparkimporter.runner.interfaces.ImportRunnerInterface;
import de.viadee.ki.sparkimporter.util.SparkImporterArguments;
import de.viadee.ki.sparkimporter.util.SparkImporterUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static de.viadee.ki.sparkimporter.SparkImporterCSVApplication.ARGS;

public class CSVImportRunner implements ImportRunnerInterface {

    private static final Logger LOG = LoggerFactory.getLogger(CSVImportRunner.class);

    @Override
    public void run(SparkSession sparkSession) {

        final long startMillis = System.currentTimeMillis();

        //Load source CSV file
        Dataset<Row> dataset = sparkSession.read()
                .option("inferSchema", "true")
                .option("delimiter", ARGS.getDelimiter())
                .option("header", "true")
                .option("ignoreLeadingWhiteSpace", "false")
                .option("ignoreTrailingWhiteSpace", "false")
                .csv(ARGS.getFileSource());

        // write imported CSV structure to file for debugging
        if (SparkImporterArguments.getInstance().isWriteStepResultsToCSV()) {
            SparkImporterUtils.getInstance().writeDatasetToCSV(dataset, "import_result");
        }

        // remove duplicated columns created at CSV import step
        dataset = SparkImporterUtils.getInstance().removeDuplicatedColumnsFromCSV(dataset);
        dataset = SparkImporterUtils.getInstance().removeEmptyLinesAfterImport(dataset);

        // write imported unique column CSV structure to file for debugging
        if (SparkImporterArguments.getInstance().isWriteStepResultsToCSV()) {
            SparkImporterUtils.getInstance().writeDatasetToCSV(dataset, "import_unique_columns_result");
        }

        Configuration config= new Configuration();
        config.createConfigFile(dataset);

        // Define preprocessing steps to run
        final PreprocessingRunner preprocessingRunner = PreprocessingRunner.getInstance();

        // it's faster if we do not reduce the dataset columns in the beginning and
        // rejoin the dataset later, left steps in commented if required later
        // preprocessingRunner.addPreprocessorStep(new ReduceColumnsDatasetStep());
        preprocessingRunner.addPreprocessorStep(new GetVariablesTypesOccurenceStep());
        preprocessingRunner.addPreprocessorStep(new VariablesTypeEscalationStep());
        preprocessingRunner.addPreprocessorStep(new AddVariablesColumnsStep());
        preprocessingRunner.addPreprocessorStep(new AggregateToProcessInstanceaStep());
        // preprocessingRunner.addPreprocessorStep(new AddRemovedColumnsToDatasetStep());
        preprocessingRunner.addPreprocessorStep(new DropColumnsStep());
        preprocessingRunner.addPreprocessorStep(new TypeCastStep());
        preprocessingRunner.addPreprocessorStep(new WriteToCSVStep());

        // Run preprocessing runner
        preprocessingRunner.run(dataset, SparkImporterArguments.getInstance().isWriteStepResultsToCSV());

        // Cleanup
        sparkSession.close();

        final long endMillis = System.currentTimeMillis();

        LOG.info("Job ran for " + ((endMillis - startMillis) / 1000) + " seconds in total.");
    }
}
