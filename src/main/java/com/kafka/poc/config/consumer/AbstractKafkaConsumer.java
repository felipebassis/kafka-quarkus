package com.kafka.poc.config.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafka.poc.Topics;
import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import javax.enterprise.event.Observes;
import javax.inject.Inject;
import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Slf4j
public abstract class AbstractKafkaConsumer<V> implements Runnable {

    private final Map<String, Object> consumerConfig = new HashMap<>();

    private final Class<V> messageClass;

    private final ExecutorService executorService = Executors.newSingleThreadExecutor();

    @Inject
    ObjectMapper objectMapper;

    protected AbstractKafkaConsumer(Class<V> messageClass) {
        consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        consumerConfig.put(ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG, true);
        consumerConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerConfig.put(ConsumerConfig.CLIENT_ID_CONFIG, "kafka-poc");
        consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, "test");
        this.messageClass = messageClass;
    }

    void onStartup(@Observes StartupEvent event) {
        this.executorService.submit(this);
    }

    void onStop(@Observes ShutdownEvent event) {
        this.executorService.shutdown();
    }

    @SuppressWarnings({"InfiniteLoopStatement", "InfiniteRecursion"})
    @Override
    public final void run() {
        try (Consumer<String, String> consumer = new KafkaConsumer<>(consumerConfig)) {
            TopicPartition topicPartition = new TopicPartition(Topics.TEST_TOPIC, 0);
            consumer.assign(Collections.singleton(topicPartition));
            consumer.seek(topicPartition, 500L);
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                records.iterator()
                        .forEachRemaining(this::consumeRecord);
            }
        } catch (KafkaException e) {
            log.error("Falha", e);
        } catch (Exception e) {
            log.error("Falha Abrupta", e);
        }
        run();
    }

    private void consumeRecord(ConsumerRecord<String, String> record) {
        try {
            V value = objectMapper.readValue(record.value(), messageClass);
            consumeRecord(value);
        } catch (IOException e) {
            log.error("Falha ao converter a mensagem para a classe {}", messageClass.getSimpleName(), e);
        }
    }

    protected abstract void consumeRecord(V record);

    protected abstract List<String> topics();
}
