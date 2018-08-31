package de.viadee.ki.sparkkafka.services;

import de.viadee.ki.sparkkafka.config.KafkaConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.Arrays;
import java.util.Collection;

@Service
public class SparkKafkaConsumerService {
        private final Logger log = LoggerFactory.getLogger(getClass());

        private final SparkConf sparkConf;
        private final KafkaConsumerConfig kafkaConsumerConfig;
        private final Collection<String> topics;

        @Autowired
        public SparkKafkaConsumerService(SparkConf sparkConf,
                                    KafkaConsumerConfig kafkaConsumerConfig,
                                    @Value("${spring.kafka.template.default-topic}") String[] topics) {
            this.sparkConf = sparkConf;
            this.kafkaConsumerConfig = kafkaConsumerConfig;
            this.topics = Arrays.asList(topics);
        }

        public void run() {
            log.debug("Running Spark Consumer Service..");

            // Create context with a 10 seconds batch interval
            JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(10));

            // Create direct kafka stream with brokers and topics
            JavaInputDStream<ConsumerRecord<String, String>> messages = KafkaUtils.createDirectStream(
                    jssc,
                    LocationStrategies.PreferConsistent(),
                    ConsumerStrategies.Subscribe(topics, kafkaConsumerConfig.consumerConfigs()));

            // Get the lines, split them into words, count the words and print
            JavaDStream<String> lines = messages.map(stringStringConsumerRecord -> stringStringConsumerRecord.value());
            //Count the tweets and print
            lines
                    .count()
                    .map(cnt -> "Ran process instances 60 seconds (" + cnt + " total process instances):")
                    .print();

            // Start the computation
            jssc.start();
            try {
                jssc.awaitTermination();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
}
