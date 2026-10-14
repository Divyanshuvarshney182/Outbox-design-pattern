package com.sheemab.inventory_service.consumer;


import com.fasterxml.jackson.databind.ObjectMapper;
import org.example.events.OrderCreatedEvent;
import com.sheemab.inventory_service.service.InventoryService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

/**
 * Listens on the "order.created" topic published by Order Service.
 *
 * On each message:
 *   1. Deserialize the payload into an OrderCreatedEvent
 *   2. Delegate to InventoryService.reserveStock() which atomically:
 *        - Reserves stock (or records failure)
 *        - Writes an outbox event so the reply is reliably sent back
 *   3. Acknowledge the Kafka offset only if everything succeeded
 *
 * Idempotency:
 *   InventoryService.reserveStock() checks the reservations table for an
 *   existing row with the same orderId before doing anything.  A duplicate
 *   Kafka delivery will hit this guard and return immediately without
 *   changing any state.
 *
 * No-acknowledge on exception:
 *   If processing throws, we intentionally skip the ack.acknowledge() call.
 *   Kafka will redeliver the message to this (or another) instance.
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class OrderEventConsumer {

    private final InventoryService inventoryService;
    private final ObjectMapper     objectMapper;

    @KafkaListener(
            topics           = "${kafka.topics.order-created:order.created}",
            groupId          = "${spring.kafka.consumer.group-id}",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void onOrderCreated(String rawMessage, Acknowledgment ack) {

        log.info("Received from order.created: {}", rawMessage);

        try {
            OrderCreatedEvent event =
                    objectMapper.readValue(rawMessage, OrderCreatedEvent.class);

            // Core work — atomic: reserve stock + write outbox event
            inventoryService.reserveStock(
                    event.getOrderId(),
                    event.getProductId(),
                    event.getQuantity()
            );

            ack.acknowledge();  // commit Kafka offset only on success
            log.info("Processed order.created: orderId={}", event.getOrderId());

        } catch (Exception ex) {
            // Do NOT acknowledge — message will be redelivered
            log.error("Failed to process order.created message — will retry. Error: {}",
                    ex.getMessage(), ex);
        }
    }
}
