package com.sheemab.order_service.service;



import com.sheemab.order_service.entity.OutboxEvent;
import com.sheemab.order_service.enums.OutboxStatus;
import com.sheemab.order_service.repository.OutboxEventRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import tools.jackson.databind.ObjectMapper;
import java.time.LocalDateTime;
import java.util.List;

@Service
@RequiredArgsConstructor
@Slf4j
public class OutboxService {

    private final OutboxEventRepository outboxEventRepository;
    private final ObjectMapper objectMapper;

    /**
     * Saves an outbox event inside the caller's existing transaction.
     * MANDATORY propagation ensures this is NEVER called without an active transaction,
     * guaranteeing the event and the business data are always committed together.

     * Saves an event into the outbox table within an existing transaction.
     */
    @Transactional(propagation = Propagation.MANDATORY) // Must be called inside an active transaction
    public OutboxEvent save(String aggregateId,
                            String aggregateType,
                            String eventType,
                            Object payload,
                            String destination) {

        try {
            // Convert event payload object into JSON string for storage
            String json = objectMapper.writeValueAsString(payload);

            // Build outbox event entity with metadata and serialized payload
            OutboxEvent event = OutboxEvent.builder()
                    .aggregateId(aggregateId)      // Business entity ID (e.g., Order ID)
                    .aggregateType(aggregateType)  // Entity type (e.g., ORDER)
                    .eventType(eventType)          // Event name (e.g., ORDER_CREATED)
                    .payload(json)                 // Serialized JSON payload
                    .destination(destination)      // Kafka topic or destination
                    .build();

            // Persist event in outbox table
            OutboxEvent saved = outboxEventRepository.save(event);

            // Debug log for traceability and observability
            log.debug("Outbox event saved: id={}, type={}, aggregate={}",
                    saved.getId(), eventType, aggregateId);

            return saved;

        } catch (Exception e) {
            // Wrap JSON serialization failure into unchecked exception to trigger transaction rollback
            throw new RuntimeException("Failed to serialize outbox payload", e);
        }
    }

    /**
     * Fetches a batch of PENDING events using pessimistic locking.
     * Safe to call from multiple service instances simultaneously.
     */
    @Transactional
    public List<OutboxEvent> fetchPendingBatch(int batchSize) {
        List<OutboxEvent> all = outboxEventRepository.findPendingWithLock(
                OutboxStatus.PENDING, LocalDateTime.now()
        );
        return all.size() > batchSize ? all.subList(0, batchSize) : all;
    }

    @Transactional
    public void markProcessing(OutboxEvent event) {
        event.setStatus(OutboxStatus.PROCESSING);
        outboxEventRepository.save(event);
    }

    @Transactional
    public void markProcessed(OutboxEvent event) {
        event.markProcessed();
        outboxEventRepository.save(event);
        log.info("Outbox event processed: id={}", event.getId());
    }

    @Transactional
    public void markFailed(OutboxEvent event, String error) {
        event.markFailed(error);
        outboxEventRepository.save(event);
        log.warn("Outbox event failed: id={}, retries={}, error={}",
                event.getId(), event.getRetryCount(), error);
    }

    @Transactional(readOnly = true)
    public long countByStatus(OutboxStatus status) {
        return outboxEventRepository.countByStatus(status);
    }

    /** Housekeeping — delete PROCESSED events older than retentionDays */
    @Transactional
    public int cleanup(int retentionDays) {
        LocalDateTime cutoff = LocalDateTime.now().minusDays(retentionDays);
        List<OutboxEvent> old = outboxEventRepository.findProcessedBefore(cutoff);
        outboxEventRepository.deleteAll(old);
        if (!old.isEmpty()) {
            log.info("Cleaned up {} old processed outbox events", old.size());
        }
        return old.size();
    }
}
