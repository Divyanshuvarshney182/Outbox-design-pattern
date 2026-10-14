package com.sheemab.order_service.schedular;


import com.sheemab.order_service.entity.OutboxEvent;
import com.sheemab.order_service.publisher.KafkaPublisher;
import com.sheemab.order_service.service.OutboxService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Polls the outbox table on a fixed delay and pushes events to Kafka.
 *
 * Why fixedDelay (not fixedRate)?
 *   fixedDelay starts counting AFTER the last execution finishes,
 *   so there is never an overlap even if processing takes longer than the interval.
 *
 * Cluster safety:
 *   OutboxService.fetchPendingBatch() uses PESSIMISTIC_WRITE lock, meaning
 *   multiple pods cannot grab the same row simultaneously.
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class OutboxPublisherScheduler {

    private final OutboxService outboxService;
    private final KafkaPublisher kafkaPublisher;

    @Value("${outbox.publisher.batch-size:10}")
    private int batchSize;

    /** Guards against overlapping runs inside a single pod */
    private final AtomicBoolean running = new AtomicBoolean(false);

    // ── Main polling loop ────────────────────────────────────────────────────

    @Scheduled(fixedDelayString = "${outbox.publisher.fixed-delay-ms:5000}",
            initialDelayString = "${outbox.publisher.initial-delay-ms:10000}")
    public void publishPendingEvents() {

        // Atomically set 'running' to true only if it is currently false; otherwise skip execution
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
                    outboxService.markProcessing(event);    // prevents other pods grabbing it
                    kafkaPublisher.publish(event);           // send to Kafka
                    outboxService.markProcessed(event);      // done ✓
                    ok++;
                } catch (Exception ex) {
                    outboxService.markFailed(event, ex.getMessage());
                    fail++;
                    log.error("Failed to publish outbox event id={}: {}", event.getId(), ex.getMessage());
                }
            }

            log.info("Outbox batch finished — ok={}, failed={}", ok, fail);

        } finally {
            running.set(false);
        }
    }

    // ── Nightly cleanup ──────────────────────────────────────────────────────

    @Scheduled(cron = "${outbox.cleanup.cron:0 0 3 * * *}")
    public void cleanupOldEvents() {
        int deleted = outboxService.cleanup(7); // keep 7 days of PROCESSED events
        log.info("Outbox cleanup: deleted {} old records", deleted);
    }
}
