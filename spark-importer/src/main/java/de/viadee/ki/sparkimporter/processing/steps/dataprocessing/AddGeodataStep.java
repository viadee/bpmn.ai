package de.viadee.ki.sparkimporter.processing.steps.dataprocessing;

import de.viadee.ki.sparkimporter.processing.interfaces.PreprocessingStepInterface;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.ArrayList;
import java.util.List;

public class AddGeodataStep implements PreprocessingStepInterface {

    @Override
    public Dataset<Row> runPreprocessingStep(Dataset<Row> dataset, boolean writeStepResultIntoFile, String dataLevel) {

    	final SparkSession sparkSession = SparkSession.builder().getOrCreate();
		
    	// TODO - adapt column name when PLZ-column exists
		String[] targetColumns = {"ext_PartnerWerkstatt_plz", "ext_partnerVersicherungsnehmer_plz"};

		// read data that has to be mapped
		Dataset plz = sparkSession.read().option("header", "true").option("delimiter", "\t").csv("C:\\Users\\B77\\Desktop\\Glasbruch-Mining\\plz\\PLZ.tab");
		for(String targetColumn : targetColumns) {
			//inner join and remove unnecessary columns
			dataset = dataset.join(plz, dataset.col(targetColumn).equalTo(plz.col("plz")), "left")
					.withColumnRenamed("lon", targetColumn + "_long")
					.withColumnRenamed("lat", targetColumn + "_lat").drop("plz").drop("Ort").drop("#loc_id");
		}

		return dataset;
    }
}
