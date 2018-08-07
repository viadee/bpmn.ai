package de.viadee.ki.sparkimporter;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;
import de.viadee.ki.sparkimporter.preprocessing.steps.GetVariablesCountStep;
import de.viadee.ki.sparkimporter.preprocessing.PreprocessingRunner;
import de.viadee.ki.sparkimporter.preprocessing.steps.GetVariablesTypesOccurenceStep;
import de.viadee.ki.sparkimporter.util.SparkImporterArguments;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.spark.sql.functions.count;

public class SparkImporterApplication {

    private static final Logger LOG = LoggerFactory.getLogger(SparkImporterApplication.class);
    private static SparkImporterArguments ARGS;

    // Nutzen von JCommander um Parameter flexibel eingeben zu können.
    private static JCommander jCommander;



    public static void main(String[] arguments){
        ARGS = SparkImporterArguments.getInstance();

        //JCommander starten
        jCommander = JCommander.newBuilder()
                .addObject(SparkImporterArguments.getInstance())
                .build();
        try {
            jCommander.parse(arguments);
        } catch (ParameterException e) {
            LOG.error("Parsing der Parameter fehlgeschlagen. Fehlermeldung: " + e.getMessage());
            jCommander.usage();
            System.exit(1);
        }


        //Spark Importer Code starter hier

        //Konfiguration wird von Umgebung geladen (z.B. via spark-submit)
        SparkSession sparkSession = SparkSession.builder().getOrCreate();

        Dataset<Row> dataset = loadCSVFile(sparkSession);

        //Define steps to run
        PreprocessingRunner preprocessingRunner = PreprocessingRunner.getInstance();
        preprocessingRunner.addPreprocessorStep(new GetVariablesCountStep());
        preprocessingRunner.addPreprocessorStep(new GetVariablesTypesOccurenceStep());
        preprocessingRunner.run(dataset, true);


        //Zieldatei löschen, falls bereits existierend
        //FileUtils.deleteQuietly(new File(ARGS.getFileDestination()));


        //Aufräumen
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
