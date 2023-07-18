package org.bez.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.logging.Logger;

public class ConsumerDemo {

    private static Logger log = Logger.getLogger(ConsumerDemo.class.getName());
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "cluster.playground.cdkt.io:9092");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");

        properties.setProperty("security.protocol", "SASL_SSL");
        properties.setProperty("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"cRKbPKyfEg0l5ATwuvi5Z\" password=\"eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJodHRwczovL2F1dGguY29uZHVrdG9yLmlvIiwic291cmNlQXBwbGljYXRpb24iOiJhZG1pbiIsInVzZXJNYWlsIjpudWxsLCJwYXlsb2FkIjp7InZhbGlkRm9yVXNlcm5hbWUiOiJjUktiUEt5ZkVnMGw1QVR3dXZpNVoiLCJvcmdhbml6YXRpb25JZCI6NzQ2NTgsInVzZXJJZCI6ODY4NzIsImZvckV4cGlyYXRpb25DaGVjayI6IjU2NTlhMWQ0LTU0ZTItNGU4ZS05YzFlLWY5NDJkYTcyNjgyNyJ9fQ.TaV5eMd4p_XhYl6RLMHS5xOLpPac_EppZeDbTCMlATM\";");
        properties.setProperty("sasl.mechanism", "PLAIN");


        // create consumer configs
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());

        String groupId = "java-demo-consumer";
        String topic = "java-demo";


        properties.setProperty("group.id", groupId);

        KafkaConsumer<String, String> demoConsumer = new KafkaConsumer<>(properties);


        demoConsumer.subscribe(List.of(topic));

        final Thread mainThread = Thread.currentThread();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                log.info("received a shutdown. waking consumer...");
                demoConsumer.wakeup();
                try {
                    mainThread.join();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            })
        );



        try{
            while(true){
                ConsumerRecords<String, String> records = demoConsumer.poll(Duration.ofMillis(3000));
                for (ConsumerRecord record : records){
                    log.info("key: "+ record.key()+" value: "+record.value());
                    log.info("  partition: "+ record.partition()+" offset: "+record.offset());
                }
            }
        }
        catch(WakeupException e){
            log.info("consumer shutdown...");
        }
        finally {
            demoConsumer.close();
            log.info("Gracefully shutting down consumer");
        }


    }
}
