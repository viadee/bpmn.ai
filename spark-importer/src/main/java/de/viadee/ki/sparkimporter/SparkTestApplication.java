package de.viadee.ki.sparkimporter;

import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.types.DataTypes;


import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.catalyst.expressions.Levenshtein;
import org.apache.spark.sql.catalyst.expressions.WindowSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.viadee.ki.sparkimporter.processing.PreprocessingRunner;
import de.viadee.ki.sparkimporter.util.SparkBroadcastHelper;
import de.viadee.ki.sparkimporter.util.SparkImporterUtils;
import de.viadee.ki.sparkimporter.util.SparkImporterVariables;
import scala.reflect.reify.phases.Calculate;
import scala.util.parsing.input.StreamReader;

import org.apache.spark.sql.expressions.Window;
import javax.validation.constraints.Max;
import org.apache.spark.sql.*;

import static org.apache.spark.sql.functions.*;
import org.apache.spark.sql.api.java.UDF1;
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
import info.debatty.java.stringsimilarity.*;

public class SparkTestApplication {

	private static final Logger LOG = LoggerFactory.getLogger(SparkTestApplication.class);

	public static void main(String[] arguments) {
		// configuration is being loaded from Environment (e.g. when using spark-submit)
		final SparkSession sparkSession = SparkSession.builder().master("local[*]").appName("Test").getOrCreate();

		// read dataset
		Dataset<Row> data = sparkSession.read().option("header", "true").option("delimiter", ";")
				.csv("C:\\Users\\B77\\Documents\\datasets\\brandsReal.csv");
		Dataset<Row> testdata = sparkSession.read().option("header", "true").option("delimiter", ";")
				.csv("C:\\Users\\B77\\Documents\\datasets\\test.csv");
		data = data.withColumn("id",
				functions.row_number().over(Window.orderBy(data.col("int_fahrzeugHerstellernameAusVertrag"))));

		// Add geodata to dataset
		 Dataset locationds = addLocationStep(testdata, true,null);
		// locationds.show();
		 
	
		Dataset brandds = mapBrandsStep(data, true, null);

		sparkSession.close();
	}

	// add geodata
	public static Dataset addLocationStep(Dataset<Row> dataset, boolean writeStepResultIntoFile, String dataLevel) {

		final SparkSession sparkSession = SparkSession.builder().getOrCreate();

		// TODO - adapt column name when PLZ-column exists
		String colname = "postleitzahl";

		// read data that has to be mapped
		Dataset plz = sparkSession.read().option("header", "true").option("delimiter", "\t")
				.csv("C:\\Users\\B77\\Desktop\\Glasbruch-Mining\\plz\\PLZ.tab");

		// inner join and remove unnecessary columns
		Dataset joinedds = dataset.join(plz, dataset.col(colname).equalTo(plz.col("plz")), "left");
		joinedds = joinedds.drop("plz").drop("Ort").drop("#loc_id");

		return joinedds;

	}

	// perform levenshtein and regexp matching
	public static Dataset<Row> mapBrandsStep(Dataset<Row> dataset, boolean writeStepResultIntoFile, String dataLevel) {

		String herstellercolumn = "int_fahrzeugHerstellernameAusVertrag";

		final SparkSession sparkSession = SparkSession.builder().getOrCreate();

		Dataset<Row> levenshteinds = LevenshteinMatching(dataset, sparkSession, herstellercolumn);
		Dataset<Row> matchedds = regexMatching(levenshteinds, sparkSession, herstellercolumn);
		matchedds.show(10000);
		return matchedds;
	}

	// Perform similarity matching of the brands using the levenshtein score
	public static Dataset<Row> LevenshteinMatching(Dataset<Row> ds, SparkSession s, String herstellercolumn) {

		// read matching data in a 2-dim array
				String fileName = "C:\\Users\\B77\\Desktop\\Glasbruch-Mining\\car_brands.csv";
				File file = new File(fileName);

				// return a 2-dimensional array of strings
				List<List<String>> brandsList = new ArrayList<>();
				Scanner inputStream;
				try {
					inputStream = new Scanner(file);
					while (inputStream.hasNext()) {
						String line = inputStream.next();
						String[] values = line.split(";");
						brandsList.add(Arrays.asList(values));
					}
					inputStream.close();
				} catch (FileNotFoundException e) {
					e.printStackTrace();
				}
				
						
	
				// create user defined function
				s.udf().register("levenshteinMatching", new UDF1<String, String>() {
					
					public String call(String column) throws Exception {
						
			
						String brandOutput = "SONSTIGE";
						// discard not useful chars
						column = column.toUpperCase();
						column = column.replaceAll("[\\-,1,2,3,4,5,6,7,8,9,0,\\.,\\,\\_,\\+,\\),\\(,/\\s/g]", "");
						
						
						int lineNo = 1;
						double score = 1;
						NormalizedLevenshtein lev = new NormalizedLevenshtein();
						
						// traverse brand list and select the brand with the best score
						for (List<String> line : brandsList) {
							int columnNo = 1;
							String brand = line.get(1);
							double levScore = lev.distance(column, brand);
				
							if(levScore < score) {
								score = levScore;
								brandOutput = brand;
							}			
							lineNo++;				
						}
						if(score > 0.4) {
							brandOutput = "SONSTIGE";
						}
						if(column.equals("") || column.equals("-")) {
							brandOutput = "UNBEKANNT";
						}

						return brandOutput;
					}
				}, DataTypes.StringType);
				
				// call UDF for specific columns	
				ds = ds.withColumn("brand",callUDF("levenshteinMatching", ds.col(herstellercolumn)));
			
				
				return ds;
	}

	// applies the regexp functions from a csv file to the brands of the dataset
	public static Dataset<Row> regexMatching(Dataset<Row> dataset, SparkSession s, String herstellercolumn) {

		// read matching data in a 2-dim array
		String fileName = "C:\\Users\\B77\\Documents\\datasets\\brandmatching.csv";
		File file = new File(fileName);

		// return a 2-dimensional array of strings
		List<List<String>> brandsRegexp = new ArrayList<>();
		Scanner inputStream;
		try {
			inputStream = new Scanner(file);
			while (inputStream.hasNext()) {
				String line = inputStream.next();
				String[] values = line.split(";");
				brandsRegexp.add(Arrays.asList(values));
			}
			inputStream.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}

		String[] columns = dataset.columns();

		// traverse the dataset
		dataset = dataset.map(row -> {

			Object[] newRow = new Object[columns.length];
			int columnCount = 0;
			for (String c : columns) {
				Object columnValue = null;

				// if brand is not matched
				if (c.equals("brand") && row.getAs(c).equals("SONSTIGE")) {

					String regexpValue = null;
					int lineNo = 1;

					// traverse the matching list
					for (List<String> line : brandsRegexp) {
						int columnNo = 1;
						String brandMatch = line.get(0);
						String regExpBrand = line.get(1);
						lineNo++;

						// replace value with regexp from matching list
						columnValue = ((String) row.getAs(herstellercolumn)).replaceAll(regExpBrand, brandMatch);

						// stop loop if value is already replaced and otherwise the value stays
						// "Sonstige"
						if ((String) row.getAs(herstellercolumn) != columnValue) {
							break;
						} else {
							columnValue = "SONSTIGE";
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

		// dataset.show(500);
		// dataset = dataset.withColumn(herstellercolumn, dataset.col("brand"));

		/*
		 * save dataset into CSV file dataset.coalesce(1) .write() .option("header",
		 * "true") .option("delimiter", ";") .option("ignoreLeadingWhiteSpace", "false")
		 * .option("ignoreTrailingWhiteSpace", "false") .mode(SaveMode.Overwrite)
		 * .csv("C:\\Users\\B77\\Desktop\\outputBrands.csv");
		 */

		return dataset;
	}

}
