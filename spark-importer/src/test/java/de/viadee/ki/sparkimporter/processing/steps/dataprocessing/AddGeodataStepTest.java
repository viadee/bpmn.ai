
package de.viadee.ki.sparkimporter.processing.steps.dataprocessing;

import static org.junit.Assert.*;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.Test;

import de.viadee.ki.sparkimporter.util.SparkImporterUtils;

public class AddGeodataStepTest {

	@Test
	public void test() throws Exception {
		
		AddGeodataStep AddGeodataStep = new AddGeodataStep();
		final SparkSession sparkSession = SparkSession.builder().getOrCreate();
		
		Dataset<Row> dataGeo = sparkSession.read().option("header", "true").option("delimiter", ";").csv("C:\\Users\\B77\\Desktop\\geoTest.csv");
			
		Dataset<Row> matchedGeoDataset = AddGeodataStep.runPreprocessingStep(dataGeo, false, "process");
		matchedGeoDataset.show(20);
		
		String hash = SparkImporterUtils.getInstance().md5CecksumOfObject(matchedGeoDataset.collect());	
			
        assertEquals("Error: Geomatching", "DF63BEADD88327E9078AF0E1E7D327CA", hash);
	
	}

}
