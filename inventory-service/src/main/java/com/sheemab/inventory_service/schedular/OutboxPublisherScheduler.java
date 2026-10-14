package com.sheemab.inventory_service.schedular;


import com.sheemab.inventory_service.entity.OutboxEvent;
import com.sheemab.inventory_service.publisher.KafkaPublisher;
import com.sheemab.inventory_service.service.OutboxService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Polls the outbox table on a fixed delay and publishes to Kafka.
 *
 * Identical design to Order Service — each service owns its own scheduler
 * operating against its own outbox_events table.
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class OutboxPublisherScheduler {

    private final OutboxService outboxService;
    private final KafkaPublisher kafkaPublisher;

    @Value("${outbox.publisher.batch-size:10}")
    private int batchSize;

    private final AtomicBoolean running = new AtomicBoolean(false);

    @Scheduled(fixedDelayString = "${outbox.publisher.fixed-delay-ms:5000}",
            initialDelayString = "${outbox.publisher.initial-delay-ms:10000}")
    public void publishPendingEvents() {

        if (!running.compareAndSet(false, true)) {
            log.debug("Scheduler already running, skipping tick");
            return;
        }

        try {
            List<OutboxEvent> batch = outboxService.fetchPendingBatch(batchSize);

            if (batch.isEmpty()) {
                log.debug("No pending outbox events");
                return;
            }

            log.info("Processing {} outbox event(s)", batch.size());
            int ok = 0, fail = 0;

            for (OutboxEvent event : batch) {
                try {
                    outboxService.markProcessing(event);
                    kafkaPublisher.publish(event);
                    outboxService.markProcessed(event);
                    ok++;
                } catch (Exception ex) {
                    outboxService.markFailed(event, ex.getMessage());
                    fail++;
                    log.error("Failed to publish outbox event id={}: {}",
                            event.getId(), ex.getMessage());
                }
            }

            log.info("Outbox batch finished — ok={}, failed={}", ok, fail);

        } finally {
            running.set(false);
        }
    }

    @Scheduled(cron = "${outbox.cleanup.cron:0 0 3 * * *}")
    public void cleanupOldEvents() {
        int deleted = outboxService.cleanup(7);
        log.info("Outbox cleanup: deleted {} old records", deleted);
    }
}
