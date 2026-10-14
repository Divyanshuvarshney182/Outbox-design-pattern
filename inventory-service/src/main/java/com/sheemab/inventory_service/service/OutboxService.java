package com.sheemab.inventory_service.service;


import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sheemab.inventory_service.entity.OutboxEvent;
import com.sheemab.inventory_service.enums.OutboxStatus;
import com.sheemab.inventory_service.repository.OutboxEventRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.List;

@Service
@RequiredArgsConstructor
@Slf4j
public class OutboxService {

    private final OutboxEventRepository outboxEventRepository;
    private final ObjectMapper objectMapper;

    /**
     * Persists an outbox event inside the CALLER'S transaction (MANDATORY).
     * This is the core guarantee of the pattern: the event row and the business
     * row (e.g. InventoryReservation) are written in the same DB transaction.
     */
    @Transactional(propagation = Propagation.MANDATORY)
    public OutboxEvent save(String aggregateId,
                            String aggregateType,
                            String eventType,
                            Object payload,
                            String destination) {
        try {
            String json = objectMapper.writeValueAsString(payload);

            OutboxEvent event = OutboxEvent.builder()
                    .aggregateId(aggregateId)
                    .aggregateType(aggregateType)
                    .eventType(eventType)
                    .payload(json)
                    .destination(destination)
                    .build();

            OutboxEvent saved = outboxEventRepository.save(event);
            log.debug("Outbox event saved: id={}, type={}, aggregate={}",
                    saved.getId(), eventType, aggregateId);
            return saved;

        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to serialize outbox payload", e);
        }
    }

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

    @Transactional
    public int cleanup(int retentionDays) {
        LocalDateTime cutoff = LocalDateTime.now().minusDays(retentionDays);
        List<OutboxEvent> old = outboxEventRepository.findProcessedBefore(cutoff);
        outboxEventRepository.deleteAll(old);
        if (!old.isEmpty()) log.info("Cleaned up {} old outbox records", old.size());
        return old.size();
    }
}
