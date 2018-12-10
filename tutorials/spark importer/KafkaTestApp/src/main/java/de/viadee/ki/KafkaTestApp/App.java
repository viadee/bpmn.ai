package de.viadee.ki.KafkaTestApp;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.stream.Stream;

import org.I0Itec.zkclient.ZkClient;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.utils.Time;
import kafka.utils.TestUtils;
import kafka.admin.AdminUtils;
import kafka.admin.RackAwareMode;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.utils.MockTime;

import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;
import kafka.zk.EmbeddedZookeeper;

public class App {
	private final static String FILE_STREAM_INPUT_PROCESS_INSTANCE = "C:\\Users\\B77\\Documents\\bpmn.ai\\tutorials\\spark importer\\variableUpdate.json";
	private final static String FILE_STREAM_INPUT_VARIABLE_UPDATE = "C:\\Users\\B77\\Documents\\bpmn.ai\\tutorials\\spark importer\\processInstance.json";
	private final static String IMPORT_TEST_OUTPUT_DIRECTORY_PROCESS = "integration-test-result-kafka-import-process";

	private final static String TOPIC_PROCESS_INSTANCE = "processInstance";
	private final static String TOPIC_VARIABLE_UPDATE = "variableUpdate";

	private final static String ZOOKEEPER_HOST = "127.0.0.1";
	private final static String KAFKA_HOST = "127.0.0.1";
	private final static String KAFKA_PORT = "19092";

	private static EmbeddedZookeeper zkServer;
	private static ZkClient zkClient;
	private static KafkaServer kafkaServer;

	public static void main(String[] args) throws IOException {

		
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
		brokerProps.setProperty("offsets.topic.replication.factor", "1");
		KafkaConfig config = new KafkaConfig(brokerProps);
		Time mock = new MockTime();
		kafkaServer = TestUtils.createServer(config, mock);

		// create topic
		AdminUtils.createTopic(zkUtils, TOPIC_PROCESS_INSTANCE, 1, 1, new Properties(),
				RackAwareMode.Disabled$.MODULE$);
		AdminUtils.createTopic(zkUtils, TOPIC_VARIABLE_UPDATE, 1, 1, new Properties(), RackAwareMode.Disabled$.MODULE$);

		// setup producer
		Properties producerProps = new Properties();
		producerProps.setProperty("bootstrap.servers", KAFKA_HOST + ":" + KAFKA_PORT);
		producerProps.setProperty("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
		producerProps.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		KafkaProducer<Integer, String> producer = new KafkaProducer<>(producerProps);

		// fill in test data
		try (Stream<String> stream = Files.lines(Paths.get(FILE_STREAM_INPUT_PROCESS_INSTANCE))) {
			stream.forEach(l -> producer.send(new ProducerRecord<>(TOPIC_PROCESS_INSTANCE, 0, 0, l)));
		}

		try (Stream<String> stream = Files.lines(Paths.get(FILE_STREAM_INPUT_VARIABLE_UPDATE))) {
			stream.forEach(l -> producer.send(new ProducerRecord<>(TOPIC_VARIABLE_UPDATE, 0, 0, l)));
		}
	}
}
