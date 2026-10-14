package com.sheemab.inventory_service.controller;


import com.sheemab.inventory_service.dto.OutboxStats;
import com.sheemab.inventory_service.enums.OutboxStatus;
import com.sheemab.inventory_service.service.OutboxService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * GET /api/outbox/stats — shows outbox health at a glance.
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
