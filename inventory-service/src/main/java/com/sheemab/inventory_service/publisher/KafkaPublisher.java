package com.sheemab.inventory_service.publisher;


import com.sheemab.inventory_service.entity.OutboxEvent;
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
     * Sends the outbox event payload to the designated Kafka topic.
     * Uses aggregateId (reservationId) as the message key so that all events
     * for the same reservation land on the same partition — preserving order.
     *
     * Blocks for up to 10 seconds awaiting broker acknowledgment.
     * Throws on timeout or broker error so the caller (scheduler) can retry.
     */
    public void publish(OutboxEvent event) throws Exception {
        String topic   = event.getDestination();
        String key     = event.getAggregateId();
        String payload = event.getPayload();

        log.debug("Publishing to Kafka: topic={}, key={}", topic, key);

        SendResult<String, String> result =
                kafkaTemplate.send(topic, key, payload).get(10, TimeUnit.SECONDS);

        log.info("Published: topic={}, partition={}, offset={}, eventId={}",
                topic,
                result.getRecordMetadata().partition(),
                result.getRecordMetadata().offset(),
                event.getId());
    }
}
