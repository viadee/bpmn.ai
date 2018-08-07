package de.viadee.ki.sparkimporter;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;
import de.viadee.ki.sparkimporter.preprocessing.steps.GetVariablesCountStep;
import de.viadee.ki.sparkimporter.preprocessing.PreprocessingRunner;
import de.viadee.ki.sparkimporter.preprocessing.steps.GetVariablesTypesOccurenceStep;
import de.viadee.ki.sparkimporter.util.SparkImporterArguments;
import org.apache.commons.io.FileUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

public class SparkImporterApplication {

    private static final Logger LOG = LoggerFactory.getLogger(SparkImporterApplication.class);
    private static SparkImporterArguments ARGS;

    // Use JCommander for flexible usage of Parameters
    private static JCommander jCommander;



    public static void main(String[] arguments){
        ARGS = SparkImporterArguments.getInstance();

        //instantiate JCommander
        jCommander = JCommander.newBuilder()
                .addObject(SparkImporterArguments.getInstance())
                .build();
        try {
            jCommander.parse(arguments);
        } catch (ParameterException e) {
            LOG.error("Parsing of parameters failed. Error message: " + e.getMessage());
            jCommander.usage();
            System.exit(1);
        }


        //SparkImporter code starts here

        //Delete destination files, required to avoid exception during runtime
        FileUtils.deleteQuietly(new File(ARGS.getFileDestination()));

        //Configuration is being loaded from Environment (e.g. when using spark-submit)
        SparkSession sparkSession = SparkSession.builder().getOrCreate();

        //laoad CSV file
        Dataset<Row> dataset = loadCSVFile(sparkSession);

        //Define steps to run
        PreprocessingRunner preprocessingRunner = PreprocessingRunner.getInstance();
        preprocessingRunner.addPreprocessorStep(new GetVariablesCountStep());
        preprocessingRunner.addPreprocessorStep(new GetVariablesTypesOccurenceStep());
        preprocessingRunner.run(dataset, true);

        //Cleanup
        sparkSession.close();
    }

    private static Dataset<Row> loadCSVFile(SparkSession sparkSession) {
        //Load source CSV file
        return sparkSession.sqlContext().read()
                .format("com.databricks.spark.csv")
                .option("inferSchema", "true")
                .option("delimiter", ARGS.getDelimiter())
                .option("header", "true")
                .load(ARGS.getFileSource());
    }


}
