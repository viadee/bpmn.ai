package de.viadee.ki.sparkimporter.util;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class SparkImporterUtils {

    private SparkImporterArguments args = SparkImporterArguments.getInstance();

    private static SparkImporterUtils instance;

    private SparkImporterUtils(){}

    public static synchronized SparkImporterUtils getInstance(){
        if(instance == null){
            instance = new SparkImporterUtils();
        }
        return instance;
    }

    public void writeDatasetToCSV(Dataset<Row> dataSet, String subDirectory) {
        //save dataset into CSV file
        dataSet.coalesce(1)
                .write()
                .format("com.databricks.spark.csv")
                .option("header", "true")
                .save(args.getFileDestination()+"/"+subDirectory);
    }
}
