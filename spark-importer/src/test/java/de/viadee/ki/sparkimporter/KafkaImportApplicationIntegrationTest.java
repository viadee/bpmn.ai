package de.viadee.ki.sparkimporter;

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

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;

public class KafkaImportApplicationIntegrationTest {

    private final static String FILE_STREAM_INPUT_PROCESS_INSTANCE = "./src/test/resources/integration_test_file_kafka_stream_processInstance.json";
    private final static String FILE_STREAM_INPUT_ACTIVITY_INSTANCE = "./src/test/resources/integration_test_file_kafka_stream_activityInstance.json";
    private final static String FILE_STREAM_INPUT_VARIABLE_UPDATE = "./src/test/resources/integration_test_file_kafka_stream_variableUpdate.json";
    private final static String IMPORT_TEST_OUTPUT_DIRECTORY_PROCESS = "integration-test-result-kafka-import-process";
    private final static String IMPORT_TEST_OUTPUT_DIRECTORY_ACTIVITY = "integration-test-result-kafka-import-activity";

    private final static String TOPIC_PROCESS_INSTANCE = "processInstance";
    private final static String TOPIC_ACTIVITY_INSTANCE = "activityInstance";
    private final static String TOPIC_VARIABLE_UPDATE = "variableUpdate";

    private final static String ZOOKEEPER_HOST = "127.0.0.1";
    private final static String KAFKA_HOST = "127.0.0.1";
    private final static String KAFKA_PORT = "19092";

    private static EmbeddedZookeeper zkServer;
    private static ZkClient zkClient;
    private static KafkaServer kafkaServer;

    @BeforeClass
    public static void setupBeforeClass() throws Exception {
        //System.setProperty("hadoop.home.dir", "C:\\Users\\b60\\Desktop\\hadoop-2.6.0\\hadoop-2.6.0");

        // setup Zookeeper
        zkServer = new EmbeddedZookeeper();
        String zkConnect = ZOOKEEPER_HOST + ":" + zkServer.port();
        zkClient = new ZkClient(zkConnect, 30000, 30000, ZKStringSerializer$.MODULE$);
        ZkUtils zkUtils = ZkUtils.apply(zkClient, false);

        // setup Kafka
        Properties brokerProps = new Properties();
        brokerProps.setProperty("zookeeper.connect", zkConnect);
        brokerProps.setProperty("broker.id", "0");
        brokerProps.setProperty("log.dirs", Files.createTempDirectory("kafka-").toAbsolutePath().toString());
        brokerProps.setProperty("listeners", "PLAINTEXT://" + KAFKA_HOST + ":" + KAFKA_PORT);
        brokerProps.setProperty("offsets.topic.replication.factor" , "1");
        KafkaConfig config = new KafkaConfig(brokerProps);
        Time mock = new MockTime();
        kafkaServer = TestUtils.createServer(config, mock);

        // create topic
        AdminUtils.createTopic(zkUtils, TOPIC_PROCESS_INSTANCE, 1, 1, new Properties(), RackAwareMode.Disabled$.MODULE$);
        AdminUtils.createTopic(zkUtils, TOPIC_ACTIVITY_INSTANCE, 1, 1, new Properties(), RackAwareMode.Disabled$.MODULE$);
        AdminUtils.createTopic(zkUtils, TOPIC_VARIABLE_UPDATE, 1, 1, new Properties(), RackAwareMode.Disabled$.MODULE$);

        // setup producer
        Properties producerProps = new Properties();
        producerProps.setProperty("bootstrap.servers", KAFKA_HOST + ":" + KAFKA_PORT);
        producerProps.setProperty("key.serializer","org.apache.kafka.common.serialization.IntegerSerializer");
        producerProps.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer<Integer, String> producer = new KafkaProducer<>(producerProps);

        //fill in test data
        try (Stream<String> stream = Files.lines(Paths.get(FILE_STREAM_INPUT_PROCESS_INSTANCE))) {
            stream.forEach(l -> producer.send(new ProducerRecord<>(TOPIC_PROCESS_INSTANCE, 0, 0, l)));
        }

        try (Stream<String> stream = Files.lines(Paths.get(FILE_STREAM_INPUT_ACTIVITY_INSTANCE))) {
            stream.forEach(l -> producer.send(new ProducerRecord<>(TOPIC_ACTIVITY_INSTANCE, 0, 0, l)));
        }

        try (Stream<String> stream = Files.lines(Paths.get(FILE_STREAM_INPUT_VARIABLE_UPDATE))) {
            stream.forEach(l -> producer.send(new ProducerRecord<>(TOPIC_VARIABLE_UPDATE, 0, 0, l)));
        }
    }

    @Test
    public void testKafkaStreamingImportProcessLevel() throws Exception {
        //run main class
        String args[] = {"-kb", KAFKA_HOST + ":" + KAFKA_PORT, "-fd", IMPORT_TEST_OUTPUT_DIRECTORY_PROCESS, "-bm", "true", "-sr", "false", "-dl", "process", "-wd", "./src/test/resources/config/kafka_import_process/", "-sm", "overwrite"};
        SparkConf sparkConf = new SparkConf();
        sparkConf.setMaster("local[*]");
        SparkSession.builder().config(sparkConf).getOrCreate();
        KafkaImportApplication.main(args);

        //start Spark session
        SparkSession sparkSession = SparkSession.builder()
                .master("local[*]")
                .appName("IntegrationTest")
                .getOrCreate();

        //generate Dataset and create hash to compare
        Dataset<Row> importedDataset = sparkSession.read().load(IMPORT_TEST_OUTPUT_DIRECTORY_PROCESS);

        //check that dataset contains 43 lines
        assertEquals(43, importedDataset.count());

        //check hash of dataset
        String hash = SparkImporterUtils.getInstance().md5CecksumOfObject(importedDataset.collect());
        assertEquals("E3DB14A986C30DC28BD91A9995353A0C", hash);

        //close Spark session
        sparkSession.close();
    }

    @Test
    public void testKafkaStreamingImportActivityLevel() throws Exception {
        //run main class
        String args[] = {"-kb", KAFKA_HOST + ":" + KAFKA_PORT, "-fd", IMPORT_TEST_OUTPUT_DIRECTORY_ACTIVITY, "-bm", "true", "-sr", "false", "-dl", "activity", "-wd", "./src/test/resources/config/kafka_import_activity/","-sm", "overwrite"};
        SparkConf sparkConf = new SparkConf();
        sparkConf.setMaster("local[*]");
        SparkSession.builder().config(sparkConf).getOrCreate();
        KafkaImportApplication.main(args);

        //start Spark session
        SparkSession sparkSession = SparkSession.builder()
                .master("local[*]")
                .appName("IntegrationTest")
                .getOrCreate();

        //generate Dataset and create hash to compare
        Dataset<Row> importedDataset = sparkSession.read().load(IMPORT_TEST_OUTPUT_DIRECTORY_ACTIVITY);

        //check that dataset contains 55 lines
        assertEquals(55, importedDataset.count());

        //check hash of dataset
        String hash = SparkImporterUtils.getInstance().md5CecksumOfObject(importedDataset.collect());
        assertEquals("A4C58B4B824734D4C51D97FA269A6EB2", hash);

        //close Spark session
        sparkSession.close();
    }

    @AfterClass
    public static void tearDown() throws Exception {
        // ignoring NoSuchErrorMethod which occurs when Zookeeper is shutting down. Does not influence tests
        try {
            kafkaServer.shutdown();
            zkClient.close();
            zkServer.shutdown();
        } catch (Exception e) {}

    }
}