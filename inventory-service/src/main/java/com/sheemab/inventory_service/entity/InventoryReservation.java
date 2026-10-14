package com.sheemab.inventory_service.entity;


import jakarta.persistence.*;
import lombok.*;
import org.hibernate.annotations.CreationTimestamp;

import java.time.LocalDateTime;

/**
 * Audit record of every reservation attempt.
 * Doubles as an idempotency key — if we see the same orderId again,
 * we return the already-stored result instead of re-processing.
 */
@Entity
@Table(
        name = "inventory_reservations",
        indexes = { @Index(name = "idx_reservation_order_id", columnList = "order_id") }
)
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class InventoryReservation {

    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    private String id;

    @Column(name = "order_id", nullable = false, unique = true)
    private String orderId;         // unique → guarantees one reservation per order

    @Column(nullable = false)
    private String productId;

    @Column(nullable = false)
    private int quantity;

    @Column(nullable = false)
    private boolean success;

    private String reason;          // populated only when success = false

    @CreationTimestamp
    @Column(nullable = false, updatable = false)
    private LocalDateTime createdAt;
}
