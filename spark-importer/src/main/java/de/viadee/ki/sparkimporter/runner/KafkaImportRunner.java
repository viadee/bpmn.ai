package de.viadee.ki.sparkimporter.runner;

import de.viadee.ki.sparkimporter.processing.PreprocessingRunner;
import de.viadee.ki.sparkimporter.processing.steps.importing.InitialCleanupStep;
import de.viadee.ki.sparkimporter.processing.steps.importing.KafkaImportStep;
import de.viadee.ki.sparkimporter.processing.steps.output.WriteToDataSinkStep;
import de.viadee.ki.sparkimporter.runner.interfaces.ImportRunnerInterface;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import java.util.*;
import java.util.concurrent.CountDownLatch;

import static de.viadee.ki.sparkimporter.KafkaImportApplication.ARGS;

public class KafkaImportRunner implements ImportRunnerInterface {

    private final static String TOPIC_PROCESS_INSTANCE = "processInstance";
    private final static String TOPIC_VARIABLE_UPDATE = "variableUpdate";

    private final Map<String, Object> kafkaConsumerConfigPI  = new HashMap<>();
    private final Map<String, Object> kafkaConsumerConfigVU  = new HashMap<>();

    private JavaRDD<String> masterRdd = null;
    private Dataset<Row> masterDataset = null;
    private SparkSession sparkSession = null;

    private List<String> receivedQueues = new ArrayList<>();

    private List<String> emptyQueues = new ArrayList<>();
    private CountDownLatch countDownLatch;

    //how many queues are we querying and expecting to be empty in batch mode
    private int EXPECTED_QUEUES_TO_BE_EMPTIED_IN_BATCH_MODE = 2;

    @Override
    public void run(SparkSession sc) {
        sparkSession = sc;
        masterRdd = sparkSession.emptyDataset(Encoders.STRING()).javaRDD();

        // if we are in batch mode we create the countdown latch so we can shutdown the streaming
        // context once the number of queues (EXPECTED_QUEUES_TO_BE_EMPTIED_IN_BATCH_MODE) are empty.
        if(ARGS.isBatchMode()) {
            countDownLatch = new CountDownLatch(1);
        }

        int duration = 5000;

        // list of host:port pairs used for establishing the initial connections to the Kafka cluster
        kafkaConsumerConfigPI.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, ARGS.getKafkaBroker());
        kafkaConsumerConfigPI.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        kafkaConsumerConfigPI.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        // allows a pool of processes to divide the work of consuming and processing records
        kafkaConsumerConfigPI.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
        // automatically reset the offset to the earliest offset
        kafkaConsumerConfigPI.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // list of host:port pairs used for establishing the initial connections to the Kafka cluster
        kafkaConsumerConfigVU.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, ARGS.getKafkaBroker());
        kafkaConsumerConfigVU.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        kafkaConsumerConfigVU.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        // allows a pool of processes to divide the work of consuming and processing records
        kafkaConsumerConfigVU.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
        // automatically reset the offset to the earliest offset
        kafkaConsumerConfigVU.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // Create context with a x seconds batch interval
        JavaSparkContext jsc = new JavaSparkContext(sparkSession.sparkContext());
        JavaStreamingContext jssc = new JavaStreamingContext(jsc, Duration.apply(duration));

        // Create direct kafka stream with brokers and topics
        JavaInputDStream<ConsumerRecord<String, String>> processInstances = KafkaUtils.createDirectStream(
                jssc,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.Subscribe(Arrays.asList(new String[]{TOPIC_PROCESS_INSTANCE}), kafkaConsumerConfigPI));

        //go through pipe elements
        processInstances
                .map(record -> record.value())
                .foreachRDD((VoidFunction<JavaRDD<String>>) stringJavaRDD -> {
                    processMasterRDD(stringJavaRDD, TOPIC_PROCESS_INSTANCE);
                });


        // Create direct kafka stream with brokers and topics
        JavaInputDStream<ConsumerRecord<String, String>> variableUpdates = KafkaUtils.createDirectStream(
                jssc,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.Subscribe(Arrays.asList(new String[]{TOPIC_VARIABLE_UPDATE}), kafkaConsumerConfigVU));

        //go through pipe elements
        variableUpdates
                .map(record -> record.value())
                .foreachRDD((VoidFunction<JavaRDD<String>>) stringJavaRDD -> {
                    processMasterRDD(stringJavaRDD, TOPIC_VARIABLE_UPDATE);
                });

        // Start the computation
        jssc.start();

        if(ARGS.isBatchMode()) {
            try {
                // wait until countdown latch has counted down
                countDownLatch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            jssc.stop(true);
        } else {
            try {
                jssc.awaitTermination();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private synchronized void processMasterRDD(JavaRDD<String> newRDD, String queue) {
        if (newRDD.count() == 0) {
            if (!emptyQueues.contains(queue)) {
                emptyQueues.add(queue);
            }
            if(emptyQueues.size() == EXPECTED_QUEUES_TO_BE_EMPTIED_IN_BATCH_MODE) {
                countDownLatch.countDown();
            }
            return;
        }

        if (!receivedQueues.contains(queue)) {
            receivedQueues.add(queue);
        }

        if (masterDataset == null) {
            if(receivedQueues.size() == 2) {
                masterRdd = masterRdd.union(newRDD);
                Dataset<String> jsonDataset = sparkSession.createDataset(masterRdd.rdd(), Encoders.STRING());
                masterDataset = sparkSession.read().json(jsonDataset);

                writeMasterDataset();
            } else {
                masterRdd = newRDD;
            }
        } else {
            Dataset<String> jsonDataset = sparkSession.createDataset(newRDD.rdd(), Encoders.STRING());
            Dataset<Row> rowDataset = sparkSession.read().json(jsonDataset);
            Dataset<Row> unionPrepDataset = sparkSession.createDataFrame(rowDataset.rdd(), masterDataset.schema());
            masterDataset = unionPrepDataset;

            writeMasterDataset();
        }
    }

    private synchronized void writeMasterDataset() {

        //go through pipe elements
        // Define processing steps to run
        final PreprocessingRunner preprocessingRunner = new PreprocessingRunner();

        PreprocessingRunner.writeStepResultsIntoFile = ARGS.isWriteStepResultsToCSV();

        // it's faster if we do not reduce the dataset columns in the beginning and
        // rejoin the dataset later, left steps in commented if required later
        preprocessingRunner.addPreprocessorStep(new KafkaImportStep());
        preprocessingRunner.addPreprocessorStep(new InitialCleanupStep());
        preprocessingRunner.addPreprocessorStep(new WriteToDataSinkStep());

        // Run processing runner
        preprocessingRunner.run(masterDataset);

    }
}
