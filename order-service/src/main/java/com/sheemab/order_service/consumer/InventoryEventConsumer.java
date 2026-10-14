package com.sheemab.order_service.consumer;


import com.fasterxml.jackson.databind.ObjectMapper;
import com.sheemab.order_service.service.OrderService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.example.events.InventoryReservedEvent;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * Listens on the "inventory.reserved" topic for replies from Inventory Service.
 *
 * On success  → confirms the order (status = CONFIRMED)
 * On failure  → cancels  the order (status = CANCELLED)
 *
 * Idempotency:
 *   We keep a Set of already-processed eventIds in memory.
 *   In production replace this with a DB table or Redis to survive restarts.
 *
 * Manual Acknowledgment:
 *   AckMode.MANUAL means the offset is only committed after we explicitly call
 *   acknowledgment.acknowledge().  If processing fails before that point, Kafka
 *   will redeliver the message.
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class InventoryEventConsumer {

    private final OrderService orderService;
    private final ObjectMapper objectMapper;

    // In-memory idempotency store (replace with Redis / DB in production)
    private final Set<String> processedEventIds =
            Collections.synchronizedSet(new HashSet<>());

    @KafkaListener(
            topics                = "${kafka.topics.inventory-reserved:inventory.reserved}",
            groupId               = "${spring.kafka.consumer.group-id}",
            containerFactory      = "kafkaListenerContainerFactory"
    )
    public void onInventoryReserved(String rawMessage, Acknowledgment ack) {

        log.info("Received from inventory.reserved: {}", rawMessage);

        try {
            InventoryReservedEvent event =
                    objectMapper.readValue(rawMessage, InventoryReservedEvent.class);

            // ── Idempotency guard ─────────────────────────────────────────────
            if (processedEventIds.contains(event.getEventId())) {
                log.warn("Duplicate inventory event ignored: eventId={}", event.getEventId());
                ack.acknowledge(); // Manually commit the Kafka offset to mark this message as successfully processed
                return;
            }

            // ── Business logic ────────────────────────────────────────────────
            if (event.isSuccess()) {
                orderService.confirmOrder(event.getOrderId(), event.getReservationId());
                log.info("Order confirmed via inventory event: orderId={}", event.getOrderId());
            } else {
                orderService.cancelOrder(event.getOrderId(), event.getReason());
                log.warn("Order cancelled via inventory event: orderId={}, reason={}",
                        event.getOrderId(), event.getReason());
            }

            processedEventIds.add(event.getEventId());
            ack.acknowledge();  // commit offset only on success

        } catch (Exception ex) {
            // Do NOT acknowledge → Kafka will redeliver this message
            log.error("Failed to process inventory.reserved message — will retry: {}", ex.getMessage(), ex);
        }
    }
}
