package com.kafka.poc.config.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Map;

public class KafkaTemplate extends KafkaProducer<String, String> {

    public KafkaTemplate(Map<String, Object> configs) {
        super(configs, new StringSerializer(), new StringSerializer());
    }
}
