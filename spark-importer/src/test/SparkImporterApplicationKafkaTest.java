package de.viadee.ki.sparkimporter;

import com.fasterxml.jackson.databind.ObjectMapper;
import de.viadee.ki.sparkimporter.events.ActivityInstanceEvent;
import de.viadee.ki.sparkimporter.events.ProcessInstanceEvent;
import de.viadee.ki.sparkimporter.events.VariableUpdateEvent;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.powermock.api.mockito.PowerMockito;

//@RunWith(PowerMockRunner.class)
public class SparkImporterApplicationKafkaTest {


    private MockConsumer consumer;
    private Collection<String> topics;

    @Before
    public void setupKafkaMock() throws Exception {

        // Mock Kafka erzeugen

        String topic0 = "processInstance";
        String topic1 = "activityInstance";
        String topic2 = "variableUpdate";

        Collection<TopicPartition> partitions = new ArrayList<>();
        Collection<String> topicsCollection = new ArrayList<>();
        partitions.add(new TopicPartition(topic0, 1));
        partitions.add(new TopicPartition(topic1, 2));
        partitions.add(new TopicPartition(topic2, 3));

        Map<TopicPartition, Long> partitionsBeginningMap = new HashMap<>();
        Map<TopicPartition, Long> partitionsEndMap = new HashMap<>();

        // Anzahl Records und Partitionen initialisieren
        long records = 20;
        for (TopicPartition partition : partitions) {
            partitionsBeginningMap.put(partition, 0l);
            partitionsEndMap.put(partition, records);
            topicsCollection.add(partition.topic());
        }

        topics = topicsCollection;

        // Mock Consumer Einstellungen
        MockConsumer<String, String> testConsumer = new MockConsumer<>(
                OffsetResetStrategy.EARLIEST);
        testConsumer.subscribe(topicsCollection);
        testConsumer.rebalance(partitions);
        testConsumer.updateBeginningOffsets(partitionsBeginningMap);
        testConsumer.updateEndOffsets(partitionsEndMap);

        // Testdaten (4 Stück)

        ConsumerRecord<String, String> record0 = new ConsumerRecord<>(topic0, 1, 1, null, generateProcessData1());
        testConsumer.addRecord(record0);
        ConsumerRecord<String, String> record1 = new ConsumerRecord<>(topic0, 1, 2, null, generateProcessData2());
        testConsumer.addRecord(record1);
        ConsumerRecord<String, String> record2 = new ConsumerRecord<>(topic1, 2, 3, null, generateActivityData());
        testConsumer.addRecord(record2);
        ConsumerRecord<String, String> record3 = new ConsumerRecord<>(topic2, 3, 4, null, generateVariableUpdateData());
        testConsumer.addRecord(record3);

        consumer = testConsumer;

    }

    @Mock
    JavaStreamingContext streamingContext;

    @Test
    public void testKafkaUtils() {

        SparkConf conf = new     SparkConf(false).setMaster("local[2]").setAppName("My app");
        JavaStreamingContext jsc = new JavaStreamingContext(conf, new Duration(1000));



        PowerMockito.mockStatic(KafkaUtils.class);

        // Geht nicht..
        //PowerMockito.when(KafkaUtils.createDirectStream(any(), any(), any())).then..



//        final JavaInputDStream<ConsumerRecord<String, String>> stream =
//                KafkaUtils.createDirectStream(
//                        streamingContext,
//                        LocationStrategies.PreferConsistent(),
//                        ConsumerStrategies.<String, String>Subscribe(topics, settings)
//                );


//        JavaDStream<String> stream1 = stream.map(
//                new Function<ConsumerRecord<String, String>, String>() {
//                    @Override
//                    public String call(ConsumerRecord<String, String> r) {
//                        return r.value();
//                    }
//                }
//        );
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