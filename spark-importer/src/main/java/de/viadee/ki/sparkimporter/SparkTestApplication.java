package de.viadee.ki.sparkimporter;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.expressions.Levenshtein;
import org.apache.spark.sql.catalyst.expressions.WindowSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static org.apache.spark.sql.functions.levenshtein;
import static org.apache.spark.sql.functions.upper;
import org.apache.spark.sql.expressions.Window;
import javax.validation.constraints.Max;
import org.apache.spark.sql.*;
import static org.apache.spark.sql.functions.length;
import static org.apache.spark.sql. functions.regexp_replace;
import static org.apache.spark.sql.functions.*;
import org.apache.spark.api.java.function.ForeachFunction;

public class SparkTestApplication {

	private static final Logger LOG = LoggerFactory.getLogger(SparkTestApplication.class);

	public static void main(String[] arguments) {
		// configuration is being loaded from Environment (e.g. when using spark-submit)
		final SparkSession sparkSession = SparkSession.builder().master("local[*]").appName("Test").getOrCreate();

		// read data
		Dataset data = sparkSession.read().option("header", "true").option("delimiter", ";").csv("C:\\Users\\B77\\Desktop\\brandsReal.csv");
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
	
	
	
	
	public static Dataset mapBrands(Dataset ds) {
		final SparkSession sparkSession = SparkSession.builder().master("local[*]").appName("Test").getOrCreate();

		Dataset levenshteinds = LevenshteinMatching(ds,sparkSession);
		Dataset regexds = regexMatching(levenshteinds, sparkSession);
		//levenshteinds.show(1000);
		
		return null;	
	}
	
	
	
	public static Dataset LevenshteinMatching(Dataset ds, SparkSession s) {
		
		// read brands
		Dataset brands = s.read().option("header", "false").option("delimiter", ",").csv("C:\\Users\\B77\\Desktop\\Glasbruch-Mining\\car_brands.csv");
	
		// compare all
		Dataset joined = ds.crossJoin(brands);
		
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
	

	public static Dataset regexMatching(Dataset ds, SparkSession s) {
		// read matching file
		Dataset matching = s.read().option("header", "true").option("delimiter", ";").csv("C:\\Users\\B77\\Desktop\\brandmatching.csv");
		
		matching.foreach((ForeachFunction<Row>) row ->  regexp_replace(ds.col("int_fahrzeugHerstellernameAusVertragModified"), row.getString(1), row.getString(0)));
		matching.foreach((ForeachFunction<Row>) row -> System.out.println(row.getString(0)));
		
		ds.show(5000);
		
	 return null;
	}

}
