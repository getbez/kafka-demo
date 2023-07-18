package org.bez.kafka;

import com.launchdarkly.eventsource.EventSource;
import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.MessageEvent;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.protocol.types.Field;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WikiMediaEventHandler implements EventHandler {

    Logger log = LoggerFactory.getLogger(WikiMediaEventHandler.class);
    private KafkaProducer<String, String> kafkaProducer;
    private String topic;

    public WikiMediaEventHandler(KafkaProducer<String, String> kafkaProducer, String topic) {
        this.kafkaProducer = kafkaProducer;
        this.topic = topic;
    }

    @Override
    public void onOpen() {
        log.info("Event handler open");
    }

    @Override
    public void onClosed() {
        kafkaProducer.close();
    }

    @Override
    public void onMessage(String event, MessageEvent messageEvent) {
        log.info("received event: {} data: {}",event, messageEvent.getData());
        kafkaProducer.send(new ProducerRecord<>(topic, messageEvent.getData()));
    }

    @Override
    public void onComment(String comment) {
        //do nothing
    }

    @Override
    public void onError(Throwable t) {
        log.error("error while handling event", t);
    }
}
