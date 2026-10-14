package com.sheemab.order_service.controller;


import com.sheemab.order_service.dto.CreateOrderRequest;
import com.sheemab.order_service.entity.Order;
import com.sheemab.order_service.service.OrderService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import java.util.List;

@RestController
@RequestMapping("/api/orders")
@RequiredArgsConstructor
@Slf4j
public class OrderController {

    private final OrderService orderService;

    // ── POST /api/orders ─────────────────────────────────────────────────────
    @PostMapping
    public ResponseEntity<Order> createOrder(@Valid @RequestBody CreateOrderRequest req) {
        Order order = orderService.createOrder(
                req.getCustomerId(),
                req.getProductId(),
                req.getProductName(),
                req.getQuantity(),
                req.getTotalAmount()
        );
        return ResponseEntity.status(HttpStatus.CREATED).body(order);
    }

    // ── GET /api/orders ──────────────────────────────────────────────────────
    @GetMapping
    public ResponseEntity<List<Order>> getAllOrders() {
        return ResponseEntity.ok(orderService.getAllOrders());
    }

    // ── GET /api/orders/{orderId} ────────────────────────────────────────────
    @GetMapping("/{orderId}")
    public ResponseEntity<Order> getOrder(@PathVariable String orderId) {
        return ResponseEntity.ok(orderService.getOrder(orderId));
    }

    // ── GET /api/orders/customer/{customerId} ────────────────────────────────
    @GetMapping("/customer/{customerId}")
    public ResponseEntity<List<Order>> getByCustomer(@PathVariable String customerId) {
        return ResponseEntity.ok(orderService.getOrdersByCustomer(customerId));
    }

}
