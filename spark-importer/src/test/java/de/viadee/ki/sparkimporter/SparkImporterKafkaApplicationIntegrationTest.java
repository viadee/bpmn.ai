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
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.dstream.ConstantInputDStream;
import org.apache.spark.streaming.kafka010.*;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.*;

import static org.mockito.ArgumentMatchers.any;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore({"javax.security.*", "javax.management.*"})
@PrepareForTest({KafkaUtils.class})
public class SparkImporterKafkaApplicationIntegrationTest {


    private MockConsumer consumer;
    private Collection<String> topics;
    private JavaStreamingContext jssc;

    @Before
    public void setupKafkaMock() throws Exception {

        // Mock Kafka erzeugen

        System.setProperty("HADOOP_USER_NAME","something-you-like");


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

    //@Test
    public void testKafkaUtils() throws Exception {

        String topic0 = "processInstance";
        String topic1 = "activityInstance";
        String topic2 = "variableUpdate";

        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName(this.getClass().getSimpleName());
        jssc = new JavaStreamingContext(conf, new Duration(1000));

        List<ConsumerRecord<String, String>> list = new ArrayList<>();
        list.add(new ConsumerRecord<>(topic0, 1, 1, null, generateProcessData1()));
        list.add(new ConsumerRecord<>(topic0, 1, 2, null, generateProcessData2()));
        list.add(new ConsumerRecord<>(topic1, 2, 3, null, generateActivityData()));
        list.add(new ConsumerRecord<>(topic2, 3, 4, null, generateVariableUpdateData()));

        Queue<JavaRDD<ConsumerRecord<String, String>>> rddQueue = new LinkedList<>();
        rddQueue.add(jssc.sparkContext().parallelize(list));
        JavaDStream<ConsumerRecord<String, String>> dStream = jssc.queueStream(rddQueue);

        PowerMockito.mockStatic(KafkaUtils.class);

        ConstantInputDStream<ConsumerRecord<String, String>> constantInputDStream = new ConstantInputDStream(jssc.ssc(), jssc.sparkContext().parallelize(list).rdd(), scala.reflect.ClassTag$.MODULE$.apply(ConsumerRecord.class));

        PowerMockito.when(KafkaUtils.createDirectStream(any(JavaStreamingContext.class), any(LocationStrategy.class), any(ConsumerStrategy.class)))
                .thenReturn(new JavaInputDStream(constantInputDStream, scala.reflect.ClassTag$.MODULE$.apply(ConsumerRecord.class)));

        JavaInputDStream<ConsumerRecord<String, String>> stream =
                KafkaUtils.createDirectStream(
                        streamingContext,
                        LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.<String, String>Subscribe(topics, null)
                );

        stream
                .map(r -> (r.value().toString()))
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