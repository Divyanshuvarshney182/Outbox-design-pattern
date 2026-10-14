package com.sheemab.order_service.entity;



import com.sheemab.order_service.enums.OutboxStatus;
import jakarta.persistence.*;
import lombok.*;
import org.hibernate.annotations.CreationTimestamp;
import org.hibernate.annotations.UpdateTimestamp;

import java.time.LocalDateTime;

@Entity
@Table(
        name = "outbox_events",
        indexes = {
                @Index(name = "idx_outbox_status_created", columnList = "status, created_at")
        }
)
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class OutboxEvent {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    /**
     * ID of the business entity this event is about (e.g., order ID)
     */
    @Column(nullable = false)
    private String aggregateId;

    /**
     * Type of business entity (e.g., "ORDER")
     */
    @Column(nullable = false)
    private String aggregateType;

    /**
     * Type of event (e.g., "ORDER_CREATED")
     */
    @Column(nullable = false)
    private String eventType;

    /**
     * JSON serialized event payload
     */
    @Column(nullable = false, columnDefinition = "TEXT")
    private String payload;

    /**
     * Kafka topic to publish to
     */
    @Column(nullable = false)
    private String destination;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    @Builder.Default
    private OutboxStatus status = OutboxStatus.PENDING;

    @Column(nullable = false)
    @Builder.Default
    private int retryCount = 0;

    @Column(nullable = false)
    @Builder.Default
    private int maxRetries = 3;

    @Column(columnDefinition = "TEXT")
    private String errorMessage;

    private LocalDateTime processedAt;

    @CreationTimestamp
    @Column(name = "created_at", nullable = false, updatable = false)
    private LocalDateTime createdAt;

    @UpdateTimestamp
    @Column(name = "updated_at", nullable = false)
    private LocalDateTime updatedAt;

    // Ensures only one scheduler instance can update this event record at a time (optimistic locking)
    @Version
    private Long version;

    public void markProcessed() {
        this.status = OutboxStatus.PROCESSED;
        this.processedAt = LocalDateTime.now();
    }

    public void markFailed(String error) {
        this.retryCount++;
        this.errorMessage = error;
        if (this.retryCount >= this.maxRetries) {
            this.status = OutboxStatus.FAILED;
        } else {
            this.status = OutboxStatus.PENDING;  // back to pending for retry
        }
    }
}
