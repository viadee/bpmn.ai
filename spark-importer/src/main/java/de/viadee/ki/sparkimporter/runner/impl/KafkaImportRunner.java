package de.viadee.ki.sparkimporter.runner.impl;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;
import de.viadee.ki.sparkimporter.processing.steps.PipelineStep;
import de.viadee.ki.sparkimporter.processing.steps.importing.ColumnsPreparationStep;
import de.viadee.ki.sparkimporter.processing.steps.importing.InitialCleanupStep;
import de.viadee.ki.sparkimporter.processing.steps.output.WriteToDataSinkStep;
import de.viadee.ki.sparkimporter.runner.SparkRunner;
import de.viadee.ki.sparkimporter.runner.config.SparkRunnerConfig;
import de.viadee.ki.sparkimporter.util.SparkImporterUtils;
import de.viadee.ki.sparkimporter.util.SparkImporterVariables;
import de.viadee.ki.sparkimporter.util.arguments.KafkaImportArguments;
import de.viadee.ki.sparkimporter.util.logging.SparkImporterLogger;
import org.apache.commons.io.FileUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.*;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;

public class KafkaImportRunner extends SparkRunner {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaImportRunner.class);

    private final Map<String, Object> kafkaConsumerConfigPI  = new HashMap<>();
    private final Map<String, Object> kafkaConsumerConfigVU  = new HashMap<>();
    private final Map<String, Object> kafkaConsumerConfigAI  = new HashMap<>();

    private Dataset<Row> masterDataset = null;

    private List<String> receivedQueues = new ArrayList<>();

    private List<String> emptyQueues = new ArrayList<>();
    private CountDownLatch countDownLatch;

    //how many queues are we querying and expecting to be empty in batch mode
    private int EXPECTED_QUEUES_TO_BE_EMPTIED_IN_BATCH_MODE = 2; // default for process

    public KafkaImportRunner() { super(); }

    public KafkaImportRunner(SparkRunnerConfig config) {
        super(config);
    }

    @Override
    protected void initialize(String[] arguments) {
        KafkaImportArguments kafkaImportArguments = KafkaImportArguments.getInstance();

        // instantiate JCommander
        // Use JCommander for flexible usage of Parameters
        final JCommander jCommander = JCommander.newBuilder().addObject(KafkaImportArguments.getInstance()).build();
        try {
            jCommander.parse(arguments);
        } catch (final ParameterException e) {
            LOG.error("Parsing of parameters failed. Error message: " + e.getMessage());
            jCommander.usage();
            System.exit(1);
        }

        //parse arguments to create SparkRunnerConfig
        kafkaImportArguments.createOrUpdateSparkRunnerConfig(this.sparkRunnerConfig);

        EXPECTED_QUEUES_TO_BE_EMPTIED_IN_BATCH_MODE = (this.sparkRunnerConfig.getDataLevel().equals(SparkImporterVariables.DATA_LEVEL_PROCESS) ? 2 : 3);

        // Delete destination files, required to avoid exception during runtime
        if(this.sparkRunnerConfig.getSaveMode().equals(SaveMode.Overwrite)) {
        	FileUtils.deleteQuietly(new File(this.sparkRunnerConfig.getTargetFolder()));
        }

        SparkImporterLogger.getInstance().writeInfo("Starting Kafka import "+ (this.sparkRunnerConfig.isBatchMode() ? "in batch mode " : "") +"from: " + this.sparkRunnerConfig.getKafkaBroker());
    }

    private synchronized void processMasterRDD(JavaRDD<String> newRDD, String queue) {
        if (newRDD.count() == 0) {
            if(this.sparkRunnerConfig.isBatchMode()) {
                SparkImporterLogger.getInstance().writeInfo("Kafka queue '" + queue + "' returned zero entries.");

                if (!emptyQueues.contains(queue)) {
                    emptyQueues.add(queue);
                }
                if(emptyQueues.size() == EXPECTED_QUEUES_TO_BE_EMPTIED_IN_BATCH_MODE) {
                    SparkImporterLogger.getInstance().writeInfo("All Kafka queues ("
                            + emptyQueues.stream().collect(Collectors.joining(","))
                            + ") returned zero entries once. Stopping as running in batch mode");
                    countDownLatch.countDown();
                }
            }

            return;
        }

        if (!receivedQueues.contains(queue)) {
            receivedQueues.add(queue);
        }

        // add source column
        Dataset<String> jd = sparkSession.createDataset(newRDD.rdd(), Encoders.STRING());
        Dataset<Row> newDataset = sparkSession.read().json(jd);
        newDataset = newDataset.withColumn("source", functions.lit(queue));

        if (masterDataset == null) {
            masterDataset = newDataset;
        } else {
            masterDataset = SparkImporterUtils.getInstance().unionDatasets(masterDataset, newDataset);
        }
    }

    @Override
    protected List<PipelineStep> buildDefaultPipeline() {
        List<PipelineStep> pipelineSteps = new ArrayList<>();

        pipelineSteps.add(new PipelineStep(new ColumnsPreparationStep(), ""));
        pipelineSteps.add(new PipelineStep(new InitialCleanupStep(), "ColumnsPreparationStep"));
        pipelineSteps.add(new PipelineStep(new WriteToDataSinkStep(), "InitialCleanupStep"));

        return pipelineSteps;
    }

    @Override
    protected Dataset<Row> loadInitialDataset() {

        final long startMillis = System.currentTimeMillis();

        // if we are in batch mode we create the countdown latch so we can shutdown the streaming
        // context once the number of queues (EXPECTED_QUEUES_TO_BE_EMPTIED_IN_BATCH_MODE) are empty.
        if(this.sparkRunnerConfig.isBatchMode()) {
            countDownLatch = new CountDownLatch(1);
        }

        int duration = 5000;

        // list of host:port pairs used for establishing the initial connections to the Kafka cluster
        kafkaConsumerConfigPI.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, this.sparkRunnerConfig.getKafkaBroker());
        kafkaConsumerConfigPI.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        kafkaConsumerConfigPI.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        // allows a pool of processes to divide the work of consuming and processing records
        kafkaConsumerConfigPI.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
        // automatically reset the offset to the earliest offset
        kafkaConsumerConfigPI.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // list of host:port pairs used for establishing the initial connections to the Kafka cluster
        kafkaConsumerConfigVU.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, this.sparkRunnerConfig.getKafkaBroker());
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
                ConsumerStrategies.Subscribe(Arrays.asList(new String[]{SparkImporterVariables.EVENT_PROCESS_INSTANCE}), kafkaConsumerConfigPI));

        //go through pipe elements
        processInstances
                .map(record -> record.value())
                .foreachRDD((VoidFunction<JavaRDD<String>>) stringJavaRDD -> {
                    processMasterRDD(stringJavaRDD, SparkImporterVariables.EVENT_PROCESS_INSTANCE);
                });

        // Create direct kafka stream with brokers and topics
        JavaInputDStream<ConsumerRecord<String, String>> variableUpdates = KafkaUtils.createDirectStream(
                jssc,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.Subscribe(Arrays.asList(new String[]{SparkImporterVariables.EVENT_VARIABLE_UPDATE}), kafkaConsumerConfigVU));

        //go through pipe elements
        variableUpdates
                .map(record -> record.value())
                .foreachRDD((VoidFunction<JavaRDD<String>>) stringJavaRDD -> {
                    processMasterRDD(stringJavaRDD, SparkImporterVariables.EVENT_VARIABLE_UPDATE);
                });


        if(this.sparkRunnerConfig.getDataLevel().equals(SparkImporterVariables.DATA_LEVEL_ACTIVITY)) {
            // list of host:port pairs used for establishing the initial connections to the Kafka cluster
            kafkaConsumerConfigAI.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, this.sparkRunnerConfig.getKafkaBroker());
            kafkaConsumerConfigAI.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
            kafkaConsumerConfigAI.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
            // allows a pool of processes to divide the work of consuming and processing records
            kafkaConsumerConfigAI.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
            // automatically reset the offset to the earliest offset
            kafkaConsumerConfigAI.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

            // Create direct kafka stream with brokers and topics
            JavaInputDStream<ConsumerRecord<String, String>> activityInstances = KafkaUtils.createDirectStream(
                    jssc,
                    LocationStrategies.PreferConsistent(),
                    ConsumerStrategies.Subscribe(Arrays.asList(new String[]{SparkImporterVariables.EVENT_ACTIVITY_INSTANCE}), kafkaConsumerConfigAI));

            //go through pipe elements
            activityInstances
                    .map(record -> record.value())
                    .foreachRDD((VoidFunction<JavaRDD<String>>) stringJavaRDD -> {
                        processMasterRDD(stringJavaRDD, SparkImporterVariables.EVENT_ACTIVITY_INSTANCE);
                    });
        }

        // Start the stream
        jssc.start();

        if(this.sparkRunnerConfig.isBatchMode()) {
            try {
                // wait until countdown latch has counted down
                countDownLatch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            final long endMillis = System.currentTimeMillis();
            SparkImporterLogger.getInstance().writeInfo("Kafka import finished (took " + ((endMillis - startMillis) / 1000) + " seconds in total)");
            jssc.stop(false);
        } else {
            try {
                jssc.awaitTermination();
                final long endMillis = System.currentTimeMillis();
                SparkImporterLogger.getInstance().writeInfo("Kafka import finished (took " + ((endMillis - startMillis) / 1000) + " seconds in total)");
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        return masterDataset;
    }
}
