package de.viadee.ki.sparkimporter;

import de.viadee.ki.sparkimporter.util.SparkImporterUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.Test;

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
        String args[] = {"-fs", DATA_PROCESSING_TEST_INPUT_DIRECTORY_PROCESS, "-fd", DATA_PROCESSING_TEST_OUTPUT_DIRECTORY_PROCESS, "-d", "|", "-sr", "false", "-sm", "overwrite", "-wd", "./src/test/resources/config/kafka_processing_process/"};
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
                .csv(DATA_PROCESSING_TEST_OUTPUT_DIRECTORY_PROCESS + "/result/result.csv");

        //check that dataset contains 4 lines
        assertEquals(4, importedDataset.count());

        //check that dataset contains 42 columns
        assertEquals(42, importedDataset.columns().length);

        //check hash of dataset
        String hash = SparkImporterUtils.getInstance().md5CecksumOfObject(importedDataset.collect());
        assertEquals("42F92CACD3DB20A399337862FE235327", hash);

    }

    @Test
    public void testKafkaDataProcessingActivityLevel() throws Exception {
        //System.setProperty("hadoop.home.dir", "C:\\Users\\b60\\Desktop\\hadoop-2.6.0\\hadoop-2.6.0");

        //run main class
        String args[] = {"-fs", DATA_PROCESSING_TEST_INPUT_DIRECTORY_ACTIVITY, "-fd", DATA_PROCESSING_TEST_OUTPUT_DIRECTORY_ACTIVITY, "-d", "|", "-sr", "false", "-dl", "activity", "-sm", "overwrite", "-wd", "./src/test/resources/config/kafka_processing_activity/"};
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
                .csv(DATA_PROCESSING_TEST_OUTPUT_DIRECTORY_ACTIVITY + "/result/result.csv");

        //check that dataset contains 12 lines
        assertEquals(12, importedDataset.count());

        //check that dataset contains 43 columns
        assertEquals(43, importedDataset.columns().length);

        //check hash of dataset
        String hash = SparkImporterUtils.getInstance().md5CecksumOfObject(importedDataset.collect());
        System.out.println(hash);
        assertEquals("F9F8EDF98F5907EDD11A886D6A1529C0", hash);

    }
}