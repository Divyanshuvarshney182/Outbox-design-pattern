package com.sheemab.order_service.publisher;


import com.sheemab.order_service.entity.OutboxEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import java.util.concurrent.TimeUnit;

@Component
@RequiredArgsConstructor
@Slf4j
public class KafkaPublisher {

    private final KafkaTemplate<String, String> kafkaTemplate;

    /**
     * Publishes the outbox event payload to Kafka synchronously (with a timeout).
     * Uses aggregateId as the Kafka message key so that all events for the
     * same order always land on the same partition, preserving ordering.
     *
     * @throws Exception if Kafka is unavailable or times out — caller handles retry
     */
    public void publish(OutboxEvent event) throws Exception {
        String topic   = event.getDestination();
        String key     = event.getAggregateId();
        String payload = event.getPayload();

        log.debug("Publishing to Kafka: topic={}, key={}", topic, key);

        SendResult<String, String> result =
                kafkaTemplate.send(topic, key, payload).get(10, TimeUnit.SECONDS);
        // Send message to Kafka and block up to 10 seconds waiting for broker acknowledgment
        // Block until Kafka confirms message delivery to safely mark event as PROCESSED

        log.info("Published: topic={}, partition={}, offset={}, eventId={}",
                topic,
                result.getRecordMetadata().partition(),
                result.getRecordMetadata().offset(),
                event.getId());
    }
}
