package de.viadee.ki.sparkimporter;

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
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.utils.Time;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

public class SparkImporterKafkaImportApplicationIntegrationTest {

    private final static String FILE_STREAM_INPUT_PROCESS_INSTANCE = "./src/test/resources/integration_test_file_kafka_stream_processInstance.json";
    private final static String FILE_STREAM_INPUT_VARIABLE_UPDATE = "./src/test/resources/integration_test_file_kafka_stream_variableUpdate.json";

    private final static String TOPIC_PROCESS_INSTANCE = "processInstance";
    private final static String TOPIC_VARIABLE_UPDATE = "variableUpdate";

    private final static String ZOOKEEPER_HOST = "127.0.0.1";
    private final static String KAFKA_HOST = "127.0.0.1";
    private final static String KAFKA_PORT = "19092";

    private static EmbeddedZookeeper zkServer;
    private static ZkClient zkClient;
    private static KafkaServer kafkaServer;

    @BeforeClass
    public static void setupKafkaMock() throws Exception {

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

        try (Stream<String> stream = Files.lines(Paths.get(FILE_STREAM_INPUT_VARIABLE_UPDATE))) {
            stream.forEach(l -> producer.send(new ProducerRecord<>(TOPIC_VARIABLE_UPDATE, 0, 0, l)));
        }
    }


    @Test
    public void testKafKaStreamingImport() throws Exception {

        // create SparkSession
        SparkSession sparkSession = SparkSession.builder()
                .master("local[*]")
                .appName(this.getClass().getSimpleName())
                .getOrCreate();

        //configuration of Kafka Consumer
        final Map<String, Object> kafkaConsumerConfig  = new HashMap<>();
        kafkaConsumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_HOST + ":" + KAFKA_PORT);
        kafkaConsumerConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        kafkaConsumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        // allows a pool of processes to divide the work of consuming and processing records
        kafkaConsumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
        // automatically reset the offset to the earliest offset
        kafkaConsumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // Create context with a x seconds batch interval
        JavaSparkContext jsc = new JavaSparkContext(sparkSession.sparkContext());
        JavaStreamingContext jssc = new JavaStreamingContext(jsc, Duration.apply(10000));

        // Create direct kafka stream with brokers and topics
        JavaInputDStream<ConsumerRecord<String, String>> stream = KafkaUtils.createDirectStream(
                jssc,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.Subscribe(Arrays.asList(new String[]{TOPIC_PROCESS_INSTANCE}), kafkaConsumerConfig));

        stream
                .map(r -> r.value())
                .print();


        // Start the computation
        jssc.start();


        CountDownLatch countDownLatch = new CountDownLatch(1);
        boolean completed = countDownLatch.await(10L, TimeUnit.SECONDS);
        jssc.stop(true);

    }

    @AfterClass
    public static void tearDown() throws Exception {
        // ignoring NoSuchErrorMethod which occurs when Zookeeper is shutting down. Does not influence tests
        try {
            kafkaServer.shutdown();
            zkClient.close();
            zkServer.shutdown();
        } catch (NoSuchMethodError e) {}

    }
}