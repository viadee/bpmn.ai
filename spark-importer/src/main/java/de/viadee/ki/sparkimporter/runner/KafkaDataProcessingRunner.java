package de.viadee.ki.sparkimporter.runner;

import de.viadee.ki.sparkimporter.processing.PreprocessingRunner;
import de.viadee.ki.sparkimporter.processing.steps.dataprocessing.AddVariablesColumnsStep;
import de.viadee.ki.sparkimporter.processing.steps.dataprocessing.AggregateToProcessInstanceaStep;
import de.viadee.ki.sparkimporter.processing.steps.dataprocessing.GetVariablesTypesOccurenceStep;
import de.viadee.ki.sparkimporter.processing.steps.dataprocessing.VariablesTypeEscalationStep;
import de.viadee.ki.sparkimporter.processing.steps.output.DataSinkFilterStep;
import de.viadee.ki.sparkimporter.processing.steps.output.WriteToCSVStep;
import de.viadee.ki.sparkimporter.runner.interfaces.ImportRunnerInterface;
import de.viadee.ki.sparkimporter.util.SparkImporterArguments;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class KafkaDataProcessingRunner implements ImportRunnerInterface {

    @Override
    public void run(SparkSession sparkSession) {

        //Load source CSV file
        Dataset<Row> dataset = sparkSession.read()
                .option("inferSchema", "true")
                .load("file:///Users/mim/Desktop/spark_sink");

        System.out.println("================ STARTING PROCESSING DATA ================");

        //go through pipe elements
        // Define processing steps to run
        final PreprocessingRunner preprocessingRunner = PreprocessingRunner.getInstance();

        // it's faster if we do not reduce the dataset columns in the beginning and
        // rejoin the dataset later, left steps in commented if required later
        preprocessingRunner.addPreprocessorStep(new DataSinkFilterStep());
        // preprocessingRunner.addPreprocessorStep(new ReduceColumnsDatasetStep());
        preprocessingRunner.addPreprocessorStep(new GetVariablesTypesOccurenceStep());
        preprocessingRunner.addPreprocessorStep(new VariablesTypeEscalationStep());
        preprocessingRunner.addPreprocessorStep(new AddVariablesColumnsStep());
        preprocessingRunner.addPreprocessorStep(new AggregateToProcessInstanceaStep());
        // preprocessingRunner.addPreprocessorStep(new AddRemovedColumnsToDatasetStep());
        preprocessingRunner.addPreprocessorStep(new WriteToCSVStep());

        // Run processing runner
        preprocessingRunner.run(dataset, SparkImporterArguments.getInstance().isWriteStepResultsToCSV());

        // Cleanup
        sparkSession.close();
    }
}
