package de.viadee.ki.sparkkafka;


import de.viadee.ki.sparkkafka.services.SparkKafkaConsumerService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class SparkKafkaConsumerApplication implements CommandLineRunner {

    @Autowired
    private SparkKafkaConsumerService sparkKafkaConsumerService;

    public static void main(String[] args){
        SpringApplication.run(SparkKafkaConsumerApplication.class, args);
    }


    @Override
    public void run(String... strings) throws Exception {
        System.setProperty("hadoop.home.dir", "C:\\Users\\b60\\Desktop\\hadoop-2.6.0\\hadoop-2.6.0");
        sparkKafkaConsumerService.run();
    }

}
