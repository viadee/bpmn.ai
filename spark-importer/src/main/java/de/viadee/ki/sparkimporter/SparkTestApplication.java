package de.viadee.ki.sparkimporter;

import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.catalyst.expressions.Levenshtein;
import org.apache.spark.sql.catalyst.expressions.WindowSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.viadee.ki.sparkimporter.util.SparkBroadcastHelper;
import de.viadee.ki.sparkimporter.util.SparkImporterUtils;
import de.viadee.ki.sparkimporter.util.SparkImporterVariables;

import org.apache.spark.sql.expressions.Window;
import javax.validation.constraints.Max;
import org.apache.spark.sql.*;

import static org.apache.spark.sql.functions.*;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.spark.api.java.function.ForeachFunction;

public class SparkTestApplication {

	private static final Logger LOG = LoggerFactory.getLogger(SparkTestApplication.class);

	public static void main(String[] arguments) {
		// configuration is being loaded from Environment (e.g. when using spark-submit)
		final SparkSession sparkSession = SparkSession.builder().master("local[*]").appName("Test").getOrCreate();

		// read data
		Dataset<Row> data = sparkSession.read().option("header", "true").option("delimiter", ";").csv("C:\\Users\\B77\\Desktop\\brandsReal.csv");
		data = data.withColumn("id", functions.row_number().over(Window.orderBy(data.col("int_fahrzeugHerstellernameAusVertrag"))));

		// Add geodata to dataset
		//Dataset p = addLocation(data, "postleitzahl");
		mapBrands(data);
			
		sparkSession.close();
	}
	
	
	
	public static Dataset addLocation(Dataset ds, String colname) {
		final SparkSession sparkSession = SparkSession.builder().master("local[*]").appName("Test").getOrCreate();
		
		// read data that has to be mapped
		Dataset plz = sparkSession.read().option("header", "true").option("delimiter", "\t").csv("C:\\Users\\B77\\Desktop\\Glasbruch-Mining\\plz\\PLZ.tab");
		
		//inner join and remove unnecessary columns
		Dataset joinedDs = ds.join(plz, ds.col(colname).equalTo(plz.col("plz")));
		joinedDs = joinedDs.drop("plz").drop("Ort").drop("#loc_id");
		
		return joinedDs;
	}
	
	
	
	// 
	public static Dataset<Row> mapBrands(Dataset<Row> ds) {
		final SparkSession sparkSession = SparkSession.builder().master("local[*]").appName("Test").getOrCreate();

		Dataset<Row> levenshteinds = LevenshteinMatching(ds,sparkSession);
		//levenshteinds.show(500);
		regexMatching(levenshteinds, sparkSession);
		//levenshteinds.show(1000);
		
		return null;	
	}
	
	
	//
	public static Dataset<Row> LevenshteinMatching(Dataset<Row> ds, SparkSession s) {
		
		// read brands
		Dataset<Row> brands = s.read().option("header", "false").option("delimiter", ",").csv("C:\\Users\\B77\\Desktop\\Glasbruch-Mining\\car_brands.csv");
	
		// compare all
		Dataset<Row> joined = ds.crossJoin(brands);
		
		// remove all special characters and convert to upper case
		joined = joined.withColumn("int_fahrzeugHerstellernameAusVertrag", upper(joined.col("int_fahrzeugHerstellernameAusVertrag")));
		joined = joined.withColumn("int_fahrzeugHerstellernameAusVertragModified", regexp_replace(joined.col("int_fahrzeugHerstellernameAusVertrag"), "[\\-,1,2,3,4,5,6,7,8,9,0,\\.,\\,\\_,\\+,\\),\\(,/\\s/g]", ""));
		joined = joined.withColumn("_c1_Modified", regexp_replace(joined.col("_c1"), "[\\-,1,2,3,4,5,6,7,8,9,0,\\.,\\,\\_,\\+,\\),\\(,/\\s/g]", ""));
		
		// calculate score
		joined = joined.withColumn("score", levenshtein(joined.col("int_fahrzeugHerstellernameAusVertragModified"), joined.col("_c1_Modified")));
		joined = joined.withColumn("length1", length(joined.col("_c1_Modified")));
		joined = joined.withColumn("length2", length(joined.col("int_fahrzeugHerstellernameAusVertragModified")));
		joined = joined.withColumn("maxLength",(when(joined.col("length1").lt(joined.col("length2")), joined.col("length2")).otherwise(joined.col("length1"))));
		joined = joined.drop("length1").drop("length2");
		joined = joined.withColumn("ratio", joined.col("score").divide(joined.col("maxLength")));
		
		// filter rows with minimal ratios
		org.apache.spark.sql.expressions.WindowSpec windowSpec = Window.partitionBy(joined.col("id")).orderBy(joined.col("ratio").asc());
		joined = joined.withColumn("minRatio", first(joined.col("ratio")).over(windowSpec).as("minRatio")).filter("ratio = minRatio");
		joined = joined.drop("ratio").drop("_c0");
		joined = joined.withColumn("brand", when(joined.col("minRatio").lt(0.5), joined.col("_c1")).otherwise("Sonstige"));
		joined = joined.orderBy(asc("id"));
		joined = joined.dropDuplicates("id");
		
		return joined;
	}
	

	/*public static Dataset regexMatching(Dataset ds, SparkSession s) {
		// read matching file
		Dataset matching = s.read().option("header", "true").option("delimiter", ";").csv("C:\\Users\\B77\\Desktop\\brandmatching.csv");
		
		ds.foreach((ForeachFunction<Row>) row ->  
		System.out.println(row.getString(0))	
		//regexp_replace(ds.col("int_fahrzeugHerstellernameAusVertragModified"), row.getString(1), row.getString(0))
		);
	
		
		ds.show(5000);
		
	 return null;
	}*/
	
	
	public static Dataset<Row> regexMatching(Dataset<Row> dataset, SparkSession s) {

		Dataset matching = s.read().option("header", "true").option("delimiter", ";").csv("C:\\Users\\B77\\Desktop\\brandmatching.csv");
		matching.show();
		//dataset.show();
		
        String[] columns = dataset.columns();
        String[] columnsMatching = matching.columns();
        
        
        dataset = dataset.map(row -> {
          
            Object[] newRow = new Object[columns.length];

            int columnCount = 0;
            for(String c : columns) {
                Object columnValue = null;
                
                // change value of brand if its value is "Sonstige" and it exists in the regex file
                
                if(c.equals("brand") ) {
                	if(row.getAs(c).equals("Sonstige") ) {
                		String regexpValue = null;
                		// regexp_replace(dataset.col("int_fahrzeugHerstellernameAusVertrag"), "", "");
                		
                		
                		//regexpValue = ;
                		
                    	//columnValue = ((String) row.getAs("int_fahrzeugHerstellernameAusVertrag")).replaceAll("-", "+");
                    	
                	}else {
                    	columnValue = row.getAs(c);
                    }	
                }
                else {
                	columnValue = row.getAs(c);
                }
                
                newRow[columnCount++] = columnValue;
            }

            return RowFactory.create(newRow);
        }, RowEncoder.apply(dataset.schema()));

      
       //dataset.show(100);
        return dataset;
    }

}
