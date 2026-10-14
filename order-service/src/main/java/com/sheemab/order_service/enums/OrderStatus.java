package com.sheemab.order_service.enums;


public enum OrderStatus {
    PENDING,       // Order placed, waiting for inventory confirmation
    CONFIRMED,     // Inventory reserved, order confirmed
    CANCELLED      // Inventory not available, order cancelled
}
