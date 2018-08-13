package de.viadee.ki.sparkimporter;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.junit.Test;

public class SparkImporterApplicationTest {

    @Test
    public void test1() {

        // Mock Kafka erzeugen

        String topic = "processInstance";
        Collection<TopicPartition> partitions = new ArrayList<>();
        Collection<String> topicsCollection = new ArrayList<>();
        partitions.add(new TopicPartition(topic, 1));
        Map<TopicPartition, Long> partitionsBeginningMap = new HashMap<>();
        Map<TopicPartition, Long> partitionsEndMap = new HashMap<>();

        // Anzahl Records und Partitionen initialisieren
        long records = 4;
        for (TopicPartition partition : partitions) {
            partitionsBeginningMap.put(partition, 0l);
            partitionsEndMap.put(partition, records);
            topicsCollection.add(partition.topic());
        }

        MockConsumer<String, String> testConsumer = new MockConsumer<>(
                OffsetResetStrategy.EARLIEST);
        testConsumer.subscribe(topicsCollection);
        testConsumer.rebalance(partitions);
        testConsumer.updateBeginningOffsets(partitionsBeginningMap);
        testConsumer.updateEndOffsets(partitionsEndMap);

        // Testdaten (4 Stück)

        ConsumerRecord<String, String> record0 = new ConsumerRecord<>(topic, 1, 1, null,"dd");
        testConsumer.addRecord(record0);
        ConsumerRecord<String, String> record1 = new ConsumerRecord<>(topic, 1, 2, null,"ddd");
        testConsumer.addRecord(record1);
        ConsumerRecord<String, String> record2 = new ConsumerRecord<>(topic, 1, 3, null,"dddd");
        testConsumer.addRecord(record2);
        ConsumerRecord<String, String> record3 = new ConsumerRecord<>(topic, 1, 4, null,"ddddd");
        testConsumer.addRecord(record3);

        // Do Stuff



    }

}