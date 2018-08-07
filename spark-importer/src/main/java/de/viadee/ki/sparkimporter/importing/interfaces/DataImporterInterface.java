package de.viadee.ki.sparkimporter.importing.interfaces;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public interface DataImporterInterface {

    Dataset<Row> importData(SparkSession sparkSession);
}
