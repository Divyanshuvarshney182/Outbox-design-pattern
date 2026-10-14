package com.sheemab.inventory_service.service;


import com.sheemab.inventory_service.entity.Inventory;
import com.sheemab.inventory_service.entity.InventoryReservation;
import org.example.events.InventoryReservedEvent;
import com.sheemab.inventory_service.repository.InventoryRepository;
import com.sheemab.inventory_service.repository.InventoryReservationRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Service
@RequiredArgsConstructor
@Slf4j
public class InventoryService {

    private final InventoryRepository inventoryRepository;
    private final InventoryReservationRepository reservationRepository;
    private final OutboxService                  outboxService;

    private static final String AGGREGATE_TYPE = "INVENTORY";
    private static final String EVENT_TYPE     = "INVENTORY_RESERVED";
    private static final String KAFKA_TOPIC    = "inventory.reserved";

    // ─────────────────────────────────────────────────────────────────────────
    //  CORE PATTERN METHOD
    //
    //  Everything below runs in a SINGLE @Transactional boundary:
    //
    //  1. Check idempotency (if we already processed this orderId, return early)
    //  2. Lock the Inventory row for this product
    //  3. Attempt to reserve stock → build InventoryReservation (success/fail)
    //  4. Persist the InventoryReservation
    //  5. Persist the OutboxEvent (via OutboxService, MANDATORY propagation)
    //
    //  If the DB commit succeeds, both rows are durable.
    //  A separate scheduler (OutboxPublisherScheduler) picks up the outbox
    //  row and publishes it to Kafka asynchronously.
    // ─────────────────────────────────────────────────────────────────────────

    @Transactional
    public InventoryReservation reserveStock(String orderId,
                                             String productId,
                                             int quantity) {

        log.info("Reserving stock: orderId={}, productId={}, qty={}", orderId, productId, quantity);

        // ── Step 1: Idempotency check ─────────────────────────────────────────
        Optional<InventoryReservation> existing = reservationRepository.findByOrderId(orderId);
        if (existing.isPresent()) {
            log.warn("Duplicate reservation request for orderId={} — returning stored result", orderId);
            return existing.get();
        }

        // ── Step 2: Lock the inventory row ────────────────────────────────────
        Optional<Inventory> inventoryOpt =
                inventoryRepository.findByProductIdWithLock(productId);

        InventoryReservation reservation;

        if (inventoryOpt.isEmpty()) {
            // Product does not exist in our catalogue
            reservation = buildReservation(orderId, productId, quantity, false,
                    "Product not found: " + productId);
            log.warn("Product not found: {}", productId);

        } else {
            Inventory inventory = inventoryOpt.get();

            if (inventory.hasStock(quantity)) {
                // ── Step 3a: Reserve stock ────────────────────────────────────
                inventory.reserve(quantity);
                inventoryRepository.save(inventory);

                reservation = buildReservation(orderId, productId, quantity, true, null);
                log.info("Stock reserved: productId={}, qty={}, remaining={}",
                        productId, quantity, inventory.getAvailableQuantity());
            } else {
                // ── Step 3b: Insufficient stock ───────────────────────────────
                String reason = "Insufficient stock for productId=%s — requested=%d, available=%d"
                        .formatted(productId, quantity, inventory.getAvailableQuantity());
                reservation = buildReservation(orderId, productId, quantity, false, reason);
                log.warn(reason);
            }
        }

        // ── Step 4: Persist the reservation ──────────────────────────────────
        InventoryReservation saved = reservationRepository.save(reservation);

        // ── Step 5: Persist outbox event in the SAME transaction ─────────────
        InventoryReservedEvent eventPayload = InventoryReservedEvent.builder()
                .eventId(UUID.randomUUID().toString())
                .reservationId(saved.getId())
                .orderId(orderId)
                .productId(productId)
                .quantity(quantity)
                .success(saved.isSuccess())
                .reason(saved.getReason())
                .reservedAt(LocalDateTime.now())
                .build();

        outboxService.save(
                saved.getId(),
                AGGREGATE_TYPE,
                EVENT_TYPE,
                eventPayload,
                KAFKA_TOPIC
        );

        log.info("Outbox event queued: orderId={}, success={}", orderId, saved.isSuccess());
        return saved;
    }

    // ── Stock management (admin / seeding endpoints) ──────────────────────────

    @Transactional
    public Inventory addStock(String productId, String productName, int quantity) {
        Optional<Inventory> existing = inventoryRepository.findByProductId(productId);

        if (existing.isPresent()) {
            Inventory inv = existing.get();
            inv.setAvailableQuantity(inv.getAvailableQuantity() + quantity);
            Inventory updated = inventoryRepository.save(inv);
            log.info("Stock topped up: productId={}, added={}, total={}",
                    productId, quantity, updated.getAvailableQuantity());
            return updated;
        }

        Inventory inv = Inventory.builder()
                .productId(productId)
                .productName(productName)
                .availableQuantity(quantity)
                .reservedQuantity(0)
                .build();
        Inventory created = inventoryRepository.save(inv);
        log.info("New inventory created: productId={}, qty={}", productId, quantity);
        return created;
    }

    @Transactional(readOnly = true)
    public Optional<Inventory> getInventory(String productId) {
        return inventoryRepository.findByProductId(productId);
    }

    @Transactional(readOnly = true)
    public List<Inventory> getAllInventory() {
        return inventoryRepository.findAll();
    }

    @Transactional(readOnly = true)
    public List<InventoryReservation> getReservationsByProduct(String productId) {
        return reservationRepository.findByProductId(productId);
    }

    // ── Private helpers ───────────────────────────────────────────────────────

    private InventoryReservation buildReservation(String orderId, String productId,
                                                  int quantity, boolean success, String reason) {
        return InventoryReservation.builder()
                .orderId(orderId)
                .productId(productId)
                .quantity(quantity)
                .success(success)
                .reason(reason)
                .build();
    }
}
