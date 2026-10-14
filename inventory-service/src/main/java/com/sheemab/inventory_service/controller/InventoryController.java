package com.sheemab.inventory_service.controller;


import com.sheemab.inventory_service.dto.StockRequest;
import com.sheemab.inventory_service.entity.Inventory;
import com.sheemab.inventory_service.entity.InventoryReservation;
import com.sheemab.inventory_service.service.InventoryService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/api/inventory")
@RequiredArgsConstructor
public class InventoryController {

    private final InventoryService inventoryService;

    // ── POST /api/inventory  ─────────────────────────────────────────────────
    // Seed stock for a product (or top up existing stock)
    @PostMapping
    public ResponseEntity<Inventory> addStock(@Valid @RequestBody StockRequest req) {
        Inventory inv = inventoryService.addStock(
                req.getProductId(), req.getProductName(), req.getQuantity());
        return ResponseEntity.ok(inv);
    }

    // ── GET /api/inventory  ──────────────────────────────────────────────────
    @GetMapping
    public ResponseEntity<List<Inventory>> getAll() {
        return ResponseEntity.ok(inventoryService.getAllInventory());
    }

    // ── GET /api/inventory/{productId}  ─────────────────────────────────────
    @GetMapping("/{productId}")
    public ResponseEntity<Inventory> getByProduct(@PathVariable String productId) {
        return inventoryService.getInventory(productId)
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }

    // ── GET /api/inventory/{productId}/reservations  ─────────────────────────
    @GetMapping("/{productId}/reservations")
    public ResponseEntity<List<InventoryReservation>> getReservations(
            @PathVariable String productId) {
        return ResponseEntity.ok(inventoryService.getReservationsByProduct(productId));
    }


}
