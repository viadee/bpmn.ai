package de.viadee.ki.sparkimporter.processing.steps.dataprocessing;

import de.viadee.ki.sparkimporter.processing.interfaces.PreprocessingStepInterface;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class AddGeodataStep implements PreprocessingStepInterface {

    @Override
    public Dataset<Row> runPreprocessingStep(Dataset<Row> dataset, boolean writeStepResultIntoFile, String dataLevel) {

    	final SparkSession sparkSession = SparkSession.builder().getOrCreate();
		
    	// TODO - adapt column name when PLZ-column exists
		String colname = "ext_PartnerWerkstatt_plz";
		
		// read data that has to be mapped
		Dataset plz = sparkSession.read().option("header", "true").option("delimiter", "\t").csv("C:\\Users\\B77\\Desktop\\Glasbruch-Mining\\plz\\PLZ.tab");
		
		//inner join and remove unnecessary columns
		Dataset joinedds = dataset.join(plz, dataset.col(colname).equalTo(plz.col("plz")));
		joinedds = joinedds.drop("plz").drop("Ort").drop("#loc_id");
		
		return joinedds;
    }
}
