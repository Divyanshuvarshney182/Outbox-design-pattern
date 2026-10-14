package com.sheemab.order_service.controller;

import com.sheemab.order_service.dto.OutboxStats;
import com.sheemab.order_service.enums.OutboxStatus;
import com.sheemab.order_service.service.OutboxService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Operational endpoint — lets you see how many outbox events are in each state.
 * Useful for health dashboards and debugging.
 *
 * GET /api/outbox/stats
 */
@RestController
@RequestMapping("/api/outbox")
@RequiredArgsConstructor
public class OutboxController {

    private final OutboxService outboxService;

    @GetMapping("/stats")
    public ResponseEntity<OutboxStats> stats() {
        OutboxStats stats = OutboxStats.builder()
                .pending(outboxService.countByStatus(OutboxStatus.PENDING))
                .processing(outboxService.countByStatus(OutboxStatus.PROCESSING))
                .processed(outboxService.countByStatus(OutboxStatus.PROCESSED))
                .failed(outboxService.countByStatus(OutboxStatus.FAILED))
                .build();
        return ResponseEntity.ok(stats);
    }
}