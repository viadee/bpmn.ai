package de.viadee.ki.sparkimporter.processing.steps.dataprocessing;

import static org.junit.Assert.*;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.Test;

import de.viadee.ki.sparkimporter.util.SparkImporterUtils;

import de.viadee.ki.sparkimporter.util.SparkImporterUtils;
import kafka.admin.AdminUtils;
import kafka.admin.RackAwareMode;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.utils.MockTime;
import kafka.utils.TestUtils;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;
import kafka.zk.EmbeddedZookeeper;
import org.I0Itec.zkclient.ZkClient;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.utils.Time;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.NoSuchAlgorithmException;
import java.util.Properties;
import java.util.stream.Stream;

public class MatchBrandsStepTest {

	@Test
	public void test() throws Exception {
		
		MatchBrandsStep MatchBrandsStep = new MatchBrandsStep();
		final SparkSession sparkSession = SparkSession.builder().master("local[*]").appName("Test").getOrCreate();
		Dataset<Row> dataLev = sparkSession.read().option("header", "true").option("delimiter", ";").csv("C:\\Users\\B77\\Documents\\datasets\\brandsTestLev.csv");
		
		Dataset<Row> dataRegexp = sparkSession.read().option("header", "true").option("delimiter", ";").csv("C:\\Users\\B77\\Documents\\datasets\\brandsTestRegexp.csv");
		
		Dataset<Row> matchedBrandDatasetLev = MatchBrandsStep.runPreprocessingStep(dataLev, false, "process");
		Dataset<Row> matchedBrandRegexp = MatchBrandsStep.runPreprocessingStep(dataRegexp, false, "process");

		String hashlev = SparkImporterUtils.getInstance().md5CecksumOfObject(matchedBrandDatasetLev.collect());	
		String hashRegexp = SparkImporterUtils.getInstance().md5CecksumOfObject(matchedBrandRegexp.collect());

        assertEquals("Error: Levenshtein brand matching", "208DE6A74B0C0A49E3A9AECFE92A2C49", hashlev);
        assertEquals("Error: Regexp brand matching","B8DEABD1375AC7D7350A479DF7D64359", hashRegexp);
	}

}
