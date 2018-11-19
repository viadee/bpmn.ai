package de.viadee.ki.sparkimporter;

import de.viadee.ki.sparkimporter.util.SparkImporterUtils;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class KafkaProcessingApplicationIntegrationTest {

    private final static String DATA_PROCESSING_TEST_OUTPUT_DIRECTORY_PROCESS = "integration-test-result-kafka-processing-process";
    private final static String DATA_PROCESSING_TEST_OUTPUT_DIRECTORY_ACTIVITY = "integration-test-result-kafka-processing-activity";
    private final static String DATA_PROCESSING_TEST_INPUT_DIRECTORY_PROCESS = "./src/test/resources/integration_test_kafka_processing_data_process";
    private final static String DATA_PROCESSING_TEST_INPUT_DIRECTORY_ACTIVITY = "./src/test/resources/integration_test_kafka_processing_data_activity";

    @Test
    public void testKafkaDataProcessingProcessLevel() throws Exception {
        //System.setProperty("hadoop.home.dir", "C:\\Users\\b60\\Desktop\\hadoop-2.6.0\\hadoop-2.6.0");

        //run main class
        String args[] = {"-fs", DATA_PROCESSING_TEST_INPUT_DIRECTORY_PROCESS, "-fd", DATA_PROCESSING_TEST_OUTPUT_DIRECTORY_PROCESS, "-d", "|", "-sr", "false", "-sm", "overwrite", "-of", "parquet", "-wd", "./src/test/resources/config/kafka_processing_process/"};
        SparkConf sparkConf = new SparkConf();
        sparkConf.setMaster("local[*]");
        SparkSession.builder().config(sparkConf).getOrCreate();

        // run main class
        KafkaProcessingApplication.main(args);

        //start Spark session
        SparkSession sparkSession = SparkSession.builder()
                .master("local[*]")
                .appName("IntegrationTest")
                .getOrCreate();

        //generate Dataset and create hash to compare
        Dataset<Row> importedDataset = sparkSession.read()
                .option("inferSchema", "true")
                .load(DATA_PROCESSING_TEST_OUTPUT_DIRECTORY_PROCESS + "/result/parquet");

        //check that dataset contains 4 lines
        assertEquals(4, importedDataset.count());

        //check that dataset contains 42 columns
        assertEquals(42, importedDataset.columns().length);

        //convert rows to string
        String[] resultLines = (String[]) importedDataset.map(row -> row.mkString(), Encoders.STRING()).collectAsList().toArray();
        for(String l : resultLines) {
            System.out.println(l);
        }

        //check if hashes of line values are correct
        //kept in for easier amendment after test case change
//        System.out.println(DigestUtils.md5Hex(resultLines[0]).toUpperCase());
//        System.out.println(DigestUtils.md5Hex(resultLines[1]).toUpperCase());
//        System.out.println(DigestUtils.md5Hex(resultLines[2]).toUpperCase());
//        System.out.println(DigestUtils.md5Hex(resultLines[3]).toUpperCase());

        assertEquals("12C8C72FB33DFEAB25514736AEEB915B", DigestUtils.md5Hex(resultLines[0]).toUpperCase());
        assertEquals("7CB6E05AA366A469DB4A4D19895AA5C2", DigestUtils.md5Hex(resultLines[1]).toUpperCase());
        assertEquals("D777617EF8DFF799BA122E3AB3EDB85C", DigestUtils.md5Hex(resultLines[2]).toUpperCase());
        assertEquals("9E4F655D37E1C247EE40908A4DFF53E2", DigestUtils.md5Hex(resultLines[3]).toUpperCase());
    }

    @Ignore
    @Test
    public void testKafkaDataProcessingActivityLevel() throws Exception {
        //System.setProperty("hadoop.home.dir", "C:\\Users\\b60\\Desktop\\hadoop-2.6.0\\hadoop-2.6.0");

        //run main class
        String args[] = {"-fs", DATA_PROCESSING_TEST_INPUT_DIRECTORY_ACTIVITY, "-fd", DATA_PROCESSING_TEST_OUTPUT_DIRECTORY_ACTIVITY, "-d", "|", "-sr", "false", "-dl", "activity", "-sm", "overwrite", "-of", "csv", "-wd", "./src/test/resources/config/kafka_processing_activity/"};
        SparkConf sparkConf = new SparkConf();
        sparkConf.setMaster("local[*]");
        SparkSession.builder().config(sparkConf).getOrCreate();

        // run main class
        KafkaProcessingApplication.main(args);

        //start Spark session
        SparkSession sparkSession = SparkSession.builder()
                .master("local[*]")
                .appName("IntegrationTest")
                .getOrCreate();

        //generate Dataset and create hash to compare
        Dataset<Row> importedDataset = sparkSession.read()
                .option("inferSchema", "true")
                .option("delimiter","|")
                .option("header", "true")
                .csv(DATA_PROCESSING_TEST_OUTPUT_DIRECTORY_ACTIVITY + "/result/csv/result.csv");

        //check that dataset contains 12 lines
        assertEquals(12, importedDataset.count());

        //check that dataset contains 43 columns
        assertEquals(43, importedDataset.columns().length);

        //check hash of dataset
        String hash = SparkImporterUtils.getInstance().md5CecksumOfObject(importedDataset.collect());
        System.out.println(hash);
        assertEquals("366577196F2B5BBC2368A16EDA429FE7", hash);

    }
}