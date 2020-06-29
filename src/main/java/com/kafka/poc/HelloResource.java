package com.kafka.poc;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafka.poc.config.producer.KafkaTemplate;
import com.kafka.poc.test.TestRecord;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import java.util.UUID;

@Path("/hello")
public class HelloResource {

    @Inject
    KafkaTemplate test;

    @Inject
    ObjectMapper objectMapper;

    @GET
    @Produces(MediaType.TEXT_PLAIN)
    public String hello() throws JsonProcessingException {
        TestRecord testRecord = new TestRecord();
        testRecord.setMessage("Test Message");
        test.send(new ProducerRecord<>(Topics.TEST_TOPIC, objectMapper.writeValueAsString(testRecord)));
        return "message sent";
    }
}