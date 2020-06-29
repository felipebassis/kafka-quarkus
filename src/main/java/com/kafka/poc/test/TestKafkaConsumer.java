package com.kafka.poc.test;

import com.kafka.poc.Topics;
import com.kafka.poc.config.consumer.AbstractKafkaConsumer;
import lombok.extern.slf4j.Slf4j;

import javax.enterprise.context.ApplicationScoped;
import java.util.List;

import static java.util.Collections.singletonList;

@Slf4j
@ApplicationScoped
public class TestKafkaConsumer extends AbstractKafkaConsumer<TestRecord> {

    public TestKafkaConsumer() {
        super(TestRecord.class);
    }

    @Override
    protected void consumeRecord(TestRecord record) {
        log.info("{}", record.getMessage());
    }

    @Override
    protected List<String> topics() {
        return singletonList(Topics.TEST_TOPIC);
    }
}
