package de.viadee.ki.sparkimporter;

import com.fasterxml.jackson.databind.ObjectMapper;
import de.viadee.ki.sparkimporter.events.ActivityInstanceEvent;
import de.viadee.ki.sparkimporter.events.ProcessInstanceEvent;
import de.viadee.ki.sparkimporter.events.VariableUpdateEvent;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.springframework.kafka.test.rule.KafkaEmbedded;
import org.springframework.kafka.test.utils.KafkaTestUtils;

import java.util.*;

public class SparkImporterKafkaApplicationIntegrationTest {

    @ClassRule
    public static KafkaEmbedded embeddedKafka = new KafkaEmbedded(
            1, true, 3, "processInstance", "variableUpdate");

    @Before
    public void setupKafkaMock() throws Exception {


        // Mock Kafka erzeugen

        String topic0 = "processInstance";
        String topic1 = "activityInstance";
        String topic2 = "variableUpdate";

        // Create Mock Data
        Map<String, Object> senderProps = KafkaTestUtils.producerProps(embeddedKafka);
        KafkaProducer<Integer, String> producer = new KafkaProducer<>(senderProps);
        producer.send(new ProducerRecord<>("processInstance", 0, 0, "message0")).get();
        producer.send(new ProducerRecord<>("processInstance", 0, 1, "message1")).get();
        producer.send(new ProducerRecord<>("processInstance", 1, 2, "message2")).get();
        producer.send(new ProducerRecord<>("processInstance", 1, 3, "message3")).get();


    }


    @Test
    public void testKafkaUtils() throws Exception {

        Map<String, Object> consumerProps =
                KafkaTestUtils.consumerProps("sampleRawConsumer", "false", embeddedKafka);

        String topic0 = "processInstance";
        String topic1 = "activityInstance";
        String topic2 = "variableUpdate";

        SparkSession sparkSession = SparkSession.builder()
                .master("local[*]")
                .appName(this.getClass().getSimpleName())
                .getOrCreate();

        List<ConsumerRecord<String, String>> list = new ArrayList<>();
        list.add(new ConsumerRecord<>(topic0, 1, 1, null, generateProcessData1()));
        list.add(new ConsumerRecord<>(topic0, 1, 2, null, generateProcessData2()));
        list.add(new ConsumerRecord<>(topic1, 2, 3, null, generateActivityData()));
        list.add(new ConsumerRecord<>(topic2, 3, 4, null, generateVariableUpdateData()));

        // list of host:port pairs used for establishing the initial connections to the Kafka cluster
        final Map<String, Object> kafkaConsumerConfig  = new HashMap<>();
        kafkaConsumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, consumerProps.get("bootstrap.servers"));
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
                ConsumerStrategies.Subscribe(Arrays.asList(new String[]{"processInstance"}), kafkaConsumerConfig));
        stream
                .map(r -> (r.value()))
                .print();


        // Start the computation
        jssc.start();
        try {
            jssc.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }

    /**
     * Objekt für die erste Prozessinstanz
     */
    private String generateProcessData1() throws Exception{
        ObjectMapper objectMapper = new ObjectMapper();

        ProcessInstanceEvent processInstanceEvent = new ProcessInstanceEvent();
        processInstanceEvent.setProcessDefinitionId("test-proc-id");
        processInstanceEvent.setProcessInstanceId("1");
        processInstanceEvent.setStartTime(new Date());
        processInstanceEvent.setState("COMPLETED");

        return objectMapper.writeValueAsString(processInstanceEvent);

    }

    /**
     * Objekt für die zweite Prozessinstanz
     */
    private String generateProcessData2() throws Exception{
        ObjectMapper objectMapper = new ObjectMapper();

        ProcessInstanceEvent processInstanceEvent = new ProcessInstanceEvent();
        processInstanceEvent.setProcessDefinitionId("test-proc-id");
        processInstanceEvent.setProcessInstanceId("2");
        processInstanceEvent.setStartTime(new Date());
        processInstanceEvent.setState("STARTED");

        return objectMapper.writeValueAsString(processInstanceEvent);

    }

    private String generateActivityData() throws Exception {
        ObjectMapper objectMapper = new ObjectMapper();

        ActivityInstanceEvent event = new ActivityInstanceEvent();
        event.setProcessDefinitionId("test-proc-id");
        event.setProcessInstanceId("1");
        event.setActivityInstanceId("1");
        event.setActivityId("my-activity");
        event.setActivityType("serviceTask");
        event.setStartTime(new Date());

        return objectMapper.writeValueAsString(event);

    }


    private String generateVariableUpdateData() throws Exception {
        ObjectMapper objectMapper = new ObjectMapper();

        VariableUpdateEvent event = new VariableUpdateEvent();
        // TODO


        return objectMapper.writeValueAsString(event);

    }




}