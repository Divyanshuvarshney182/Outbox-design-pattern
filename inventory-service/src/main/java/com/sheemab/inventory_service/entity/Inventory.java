package com.sheemab.inventory_service.entity;


import jakarta.persistence.*;
import lombok.*;
import org.hibernate.annotations.CreationTimestamp;
import org.hibernate.annotations.UpdateTimestamp;

import java.time.LocalDateTime;

@Entity
@Table(
        name = "inventory",
        indexes = { @Index(name = "idx_inventory_product_id", columnList = "product_id") }
)
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Inventory {

    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    private String id;

    @Column(name = "product_id", nullable = false, unique = true)
    private String productId;

    @Column(nullable = false)
    private String productName;

    /**
     * Quantity available for new reservations.
     */
    @Column(nullable = false)
    private int availableQuantity;

    /**
     * Quantity currently held by in-flight orders (reserved but not yet shipped).
     */
    @Column(nullable = false)
    private int reservedQuantity;

    @CreationTimestamp
    @Column(nullable = false, updatable = false)
    private LocalDateTime createdAt;

    @UpdateTimestamp
    @Column(nullable = false)
    private LocalDateTime updatedAt;

    @Version
    private Long version;   // optimistic locking

    // ── Domain helpers ────────────────────────────────────────────────────────

    public boolean hasStock(int qty) {
        return availableQuantity >= qty;
    }

    /**
     * Moves qty from available → reserved bucket.
     * Throws if stock is insufficient (caller must have checked first).
     */
    public void reserve(int qty) {
        if (!hasStock(qty)) {
            throw new IllegalStateException(
                    "Cannot reserve %d units — only %d available for productId=%s"
                            .formatted(qty, availableQuantity, productId));
        }
        availableQuantity -= qty;
        reservedQuantity  += qty;
    }

    /**
     * Moves qty back from reserved → available (e.g., on order cancellation).
     */
    public void release(int qty) {
        reservedQuantity  -= qty;
        availableQuantity += qty;
    }
}
