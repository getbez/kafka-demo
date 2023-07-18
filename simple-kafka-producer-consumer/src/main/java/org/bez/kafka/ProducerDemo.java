package org.bez.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;


public class ProducerDemo {

    private static Logger log = LoggerFactory.getLogger(ProducerDemo.class.getName());
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "cluster.playground.cdkt.io:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        properties.setProperty("security.protocol", "SASL_SSL");
        properties.setProperty("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"cRKbPKyfEg0l5ATwuvi5Z\" password=\"eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJodHRwczovL2F1dGguY29uZHVrdG9yLmlvIiwic291cmNlQXBwbGljYXRpb24iOiJhZG1pbiIsInVzZXJNYWlsIjpudWxsLCJwYXlsb2FkIjp7InZhbGlkRm9yVXNlcm5hbWUiOiJjUktiUEt5ZkVnMGw1QVR3dXZpNVoiLCJvcmdhbml6YXRpb25JZCI6NzQ2NTgsInVzZXJJZCI6ODY4NzIsImZvckV4cGlyYXRpb25DaGVjayI6IjU2NTlhMWQ0LTU0ZTItNGU4ZS05YzFlLWY5NDJkYTcyNjgyNyJ9fQ.TaV5eMd4p_XhYl6RLMHS5xOLpPac_EppZeDbTCMlATM\";");
        properties.setProperty("sasl.mechanism", "PLAIN");

        KafkaProducer<String,String> demoProducer = new KafkaProducer<>(properties);
        String topic = "java-demo";
        for(int i=0; i<=2; i++){
            for(int j=0; j<1000; j++){
                demoProducer.send(new ProducerRecord<>(topic, "id" + i, "hello_" + j), (metadata, exception) -> {
                    if(exception==null){
                       log.info("Metadata: "
                               + "\n topic: "+ metadata.topic()
                               + "\n partition: "+ metadata.partition()
                               + "\n offset: "+ metadata.offset()
                               + "\n timestamp: "+ metadata.timestamp()
                       );
                    }
                });
            }

            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        demoProducer.flush();
        demoProducer.close();

    }
}
