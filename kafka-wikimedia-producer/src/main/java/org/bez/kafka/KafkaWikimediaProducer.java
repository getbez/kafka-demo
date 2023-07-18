package org.bez.kafka;

import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.EventSource;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.net.URI;
import java.net.URL;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class KafkaWikimediaProducer {
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "cluster.playground.cdkt.io:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        properties.setProperty("security.protocol", "SASL_SSL");
        properties.setProperty("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"cRKbPKyfEg0l5ATwuvi5Z\" password=\"eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJodHRwczovL2F1dGguY29uZHVrdG9yLmlvIiwic291cmNlQXBwbGljYXRpb24iOiJhZG1pbiIsInVzZXJNYWlsIjpudWxsLCJwYXlsb2FkIjp7InZhbGlkRm9yVXNlcm5hbWUiOiJjUktiUEt5ZkVnMGw1QVR3dXZpNVoiLCJvcmdhbml6YXRpb25JZCI6NzQ2NTgsInVzZXJJZCI6ODY4NzIsImZvckV4cGlyYXRpb25DaGVjayI6IjU2NTlhMWQ0LTU0ZTItNGU4ZS05YzFlLWY5NDJkYTcyNjgyNyJ9fQ.TaV5eMd4p_XhYl6RLMHS5xOLpPac_EppZeDbTCMlATM\";");
        properties.setProperty("sasl.mechanism", "PLAIN");

        KafkaProducer<String,String> demoProducer = new KafkaProducer<>(properties);
        String topic = "wikimedia-changes";

        EventHandler wikimediaChangesHandler = new WikiMediaEventHandler(demoProducer, topic);

        URI uri = URI.create("https://stream.wikimedia.org/v2/stream/recentchange");
        EventSource eventSource = new EventSource.Builder(wikimediaChangesHandler, uri).build();

        eventSource.start();

        try {
            TimeUnit.MINUTES.sleep(5);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
