package com.sheemab.order_service.service;


import com.sheemab.order_service.entity.Order;
import com.sheemab.order_service.enums.OrderStatus;
import com.sheemab.order_service.repository.OrderRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.example.events.OrderCreatedEvent;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.util.List;
import java.util.UUID;

@Service
@RequiredArgsConstructor
@Slf4j
public class OrderService {

    private final OrderRepository orderRepository;
    private final OutboxService      outboxService;

    private static final String AGGREGATE_TYPE = "ORDER";
    private static final String EVENT_TYPE     = "ORDER_CREATED";
    private static final String KAFKA_TOPIC    = "order.created";

    /**
     * ============================  CORE PATTERN  ============================
     *
     *  1. Save the Order to the orders table.
     *  2. Save an OutboxEvent to the outbox_events table.
     *
     *  Both happen inside a SINGLE @Transactional boundary.
     *  If either step fails the entire transaction rolls back — no orphaned
     *  events, no lost events.
     *
     *  A separate scheduler (OutboxPublisherScheduler) polls the outbox table
     *  and publishes to Kafka asynchronously.
     *
     * ========================================================================
     */
    @Transactional
    public Order createOrder(String customerId,
                             String productId,
                             String productName,
                             Integer quantity,
                             BigDecimal totalAmount) {

        // ── Step 1: persist the business entity ──────────────────────────────
        Order order = Order.builder()
                .customerId(customerId)
                .productId(productId)
                .productName(productName)
                .quantity(quantity)
                .totalAmount(totalAmount)
                .status(OrderStatus.PENDING)
                .build();

        Order saved = orderRepository.save(order);
        log.info("Order saved: orderId={}, customerId={}", saved.getId(), customerId);

        // ── Step 2: persist the outbox event (same transaction) ───────────────
        OrderCreatedEvent eventPayload = OrderCreatedEvent.builder()
                .eventId(UUID.randomUUID().toString())
                .orderId(saved.getId())
                .customerId(saved.getCustomerId())
                .productId(saved.getProductId())
                .productName(saved.getProductName())
                .quantity(saved.getQuantity())
                .totalAmount(saved.getTotalAmount())
                .createdAt(saved.getCreatedAt())
                .build();

        outboxService.save(
                saved.getId(),
                AGGREGATE_TYPE,
                EVENT_TYPE,
                eventPayload,
                KAFKA_TOPIC
        );

        log.info("Outbox event queued for orderId={}", saved.getId());
        return saved;
    }

    /**
     * Called by InventoryEventConsumer when Inventory Service replies with
     * a successful reservation.
     */
    @Transactional
    public Order confirmOrder(String orderId, String reservationId) {
        Order order = findOrThrow(orderId);
        order.setStatus(OrderStatus.CONFIRMED);
        order.setInventoryReservationId(reservationId);
        log.info("Order CONFIRMED: orderId={}, reservationId={}", orderId, reservationId);
        return orderRepository.save(order);
    }

    /**
     * Called by InventoryEventConsumer when stock is insufficient.
     */
    @Transactional
    public Order cancelOrder(String orderId, String reason) {
        Order order = findOrThrow(orderId);
        order.setStatus(OrderStatus.CANCELLED);
        log.warn("Order CANCELLED: orderId={}, reason={}", orderId, reason);
        return orderRepository.save(order);
    }

    @Transactional(readOnly = true)
    public Order getOrder(String orderId) {
        return findOrThrow(orderId);
    }

    @Transactional(readOnly = true)
    public List<Order> getAllOrders() {
        return orderRepository.findAll();
    }

    @Transactional(readOnly = true)
    public List<Order> getOrdersByCustomer(String customerId) {
        return orderRepository.findByCustomerId(customerId);
    }

    // ─────────────────────────────────────────────────────────────────────────
    private Order findOrThrow(String orderId) {
        return orderRepository.findById(orderId)
                .orElseThrow(() -> new IllegalArgumentException("Order not found: " + orderId));
    }
}
