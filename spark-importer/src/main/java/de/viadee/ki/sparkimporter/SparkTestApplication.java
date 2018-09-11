package de.viadee.ki.sparkimporter;

import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.catalyst.expressions.Levenshtein;
import org.apache.spark.sql.catalyst.expressions.WindowSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.viadee.ki.sparkimporter.processing.PreprocessingRunner;
import de.viadee.ki.sparkimporter.util.SparkBroadcastHelper;
import de.viadee.ki.sparkimporter.util.SparkImporterUtils;
import de.viadee.ki.sparkimporter.util.SparkImporterVariables;
import scala.util.parsing.input.StreamReader;

import org.apache.spark.sql.expressions.Window;
import javax.validation.constraints.Max;
import org.apache.spark.sql.*;

import static org.apache.spark.sql.functions.*;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;

import org.apache.spark.api.java.function.ForeachFunction;

public class SparkTestApplication {

	private static final Logger LOG = LoggerFactory.getLogger(SparkTestApplication.class);

	
	public static void main(String[] arguments) {
		// configuration is being loaded from Environment (e.g. when using spark-submit)
		final SparkSession sparkSession = SparkSession.builder().master("local[*]").appName("Test").getOrCreate();

		// read dataset
		Dataset<Row> data = sparkSession.read().option("header", "true").option("delimiter", ";").csv("C:\\Users\\B77\\Desktop\\brandsReal.csv");
		Dataset<Row> testdata = sparkSession.read().option("header", "true").option("delimiter", ";").csv("C:\\Users\\B77\\Desktop\\test.csv");
		data = data.withColumn("id", functions.row_number().over(Window.orderBy(data.col("int_fahrzeugHerstellernameAusVertrag"))));

		// Add geodata to dataset
		 //locationds = addLocationStep(testdata, "postleitzahl");
		//locationds.show();
		Dataset brandds = mapBrandsStep(data, true, null); //data, "int_fahrzeugHerstellernameAusVertrag");
	
		sparkSession.close();
	}
	
	
	// add geodata 
	public static Dataset addLocationStep(Dataset<Row> dataset, boolean writeStepResultIntoFile, String dataLevel) {
		
		final SparkSession sparkSession = SparkSession.builder().master("local[*]").appName("Test").getOrCreate();
		
		String colname = null;
		
		// read data that has to be mapped
		Dataset plz = sparkSession.read().option("header", "true").option("delimiter", "\t").csv("C:\\Users\\B77\\Desktop\\Glasbruch-Mining\\plz\\PLZ.tab");
		
		//inner join and remove unnecessary columns
		Dataset joinedDs = dataset.join(plz, dataset.col(colname).equalTo(plz.col("plz")));
		joinedDs = joinedDs.drop("plz").drop("Ort").drop("#loc_id");
		
		return joinedDs;
	}
	
	
	
	// perform levenshtein and regexp matching
	public static Dataset<Row> mapBrandsStep(Dataset<Row> dataset, boolean writeStepResultIntoFile, String dataLevel) {
		
		String herstellercolumn = "int_fahrzeugHerstellernameAusVertrag";
				
		final SparkSession sparkSession = SparkSession.builder().master("local[*]").appName("Test").getOrCreate();

		Dataset<Row> levenshteinds = LevenshteinMatching(dataset,sparkSession, herstellercolumn);
		Dataset<Row> matchedds = regexMatching(levenshteinds, sparkSession, herstellercolumn);
		
		return matchedds;	
	}
	
	
	// Perform similarity matching of the brands using the levenshtein score
	public static Dataset<Row> LevenshteinMatching(Dataset<Row> ds, SparkSession s, String herstellercolumn) {
		
		// read brands
		Dataset<Row> brands = s.read().option("header", "false").option("delimiter", ",").csv("C:\\Users\\B77\\Desktop\\Glasbruch-Mining\\car_brands.csv");
		brands.show(20);
		ds.show(20);
		// compare all
		Dataset<Row> joined = ds.crossJoin(brands);
		
		// remove all special characters and convert to upper case
		joined = joined.withColumn(herstellercolumn, upper(joined.col(herstellercolumn)));
		joined = joined.withColumn("int_fahrzeugHerstellernameAusVertragModified", regexp_replace(joined.col(herstellercolumn), "[\\-,1,2,3,4,5,6,7,8,9,0,\\.,\\,\\_,\\+,\\),\\(,/\\s/g]", ""));
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
	


	
	// applies the regexp functions from a csv file to the brands of the dataset
	public static Dataset<Row> regexMatching(Dataset<Row> dataset, SparkSession s, String herstellercolumn) {
		
		// read matching data in a 2-dim array
		String fileName= "C:\\Users\\B77\\Desktop\\brandmatching.csv";
        File file = new File(fileName);

        // return a 2-dimensional array of strings
        List<List<String>> lines = new ArrayList<>();
        Scanner inputStream;
        try{
            inputStream = new Scanner(file);
            while(inputStream.hasNext()){
                String line= inputStream.next();
                String[] values = line.split(";");
                lines.add(Arrays.asList(values));
            }
            inputStream.close();
        }catch (FileNotFoundException e) {
            e.printStackTrace();
        }

   
        String[] columns = dataset.columns();
          
        // traverse the dataset
        dataset = dataset.map(row -> {
          
        	Object[] newRow = new Object[columns.length];	        
            int columnCount = 0;
            for(String c : columns) {
                Object columnValue = null;
                       
                // if brand is not matched
                if(c.equals("brand") && row.getAs(c).equals("Sonstige")) {
              
	        		String regexpValue = null;
	        		int lineNo = 1;
	        
	        		// traverse the matching list
	                for(List<String> line: lines) {
	                    int columnNo = 1;
	                    String brandMatch = line.get(0);
	                    String regExpBrand = line.get(1);
	                    lineNo++;
	                    
	                	// replace value with regexp from matching list
	                	columnValue = ((String) row.getAs(herstellercolumn)).replaceAll(regExpBrand, brandMatch);
	                	
	                	// stop loop if value is already replaced and otherwise the value stays "Sonstige"
	                	if( (String) row.getAs(herstellercolumn) !=  columnValue) {
	                		break;
	                	}else {
	                		columnValue = "Sonstige";
	                	}
	                }	                     	                   	
                }
                // the value of all the other columns stay the same
                else {
                	columnValue = row.getAs(c);
                }
                
                newRow[columnCount++] = columnValue;
            }

            return RowFactory.create(newRow);
        }, RowEncoder.apply(dataset.schema()));

        dataset = dataset.drop("_c1").drop("score").drop("maxLength").drop("_c1_Modified").drop("minRatio").drop("int_fahrzeugHerstellernameAusVertragModified");
       
        /*save dataset into CSV file
       dataset.coalesce(1)
               .write()
               .option("header", "true")
               .option("delimiter", ";")
               .option("ignoreLeadingWhiteSpace", "false")
               .option("ignoreTrailingWhiteSpace", "false")
               .mode(SaveMode.Overwrite)
               .csv("C:\\Users\\B77\\Desktop\\outputBrands.csv");*/


       return dataset;
    }

}
