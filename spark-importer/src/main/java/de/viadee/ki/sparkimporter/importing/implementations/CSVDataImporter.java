package de.viadee.ki.sparkimporter.importing.implementations;

import de.viadee.ki.sparkimporter.importing.interfaces.DataImporterInterface;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static de.viadee.ki.sparkimporter.SparkImporterApplication.ARGS;

public class CSVDataImporter implements DataImporterInterface {

    @Override
    public Dataset<Row> importData(SparkSession sparkSession) {
        //Load source CSV file
        return sparkSession.read()
                .option("inferSchema", "true")
                .option("delimiter", ARGS.getDelimiter())
                .option("header", "true")
                .option("ignoreLeadingWhiteSpace", "false")
                .option("ignoreTrailingWhiteSpace", "false")
                .csv(ARGS.getFileSource());
    }
}
