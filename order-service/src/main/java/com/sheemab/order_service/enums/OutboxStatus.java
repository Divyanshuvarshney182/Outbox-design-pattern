package com.sheemab.order_service.enums;

public enum OutboxStatus {
    PENDING,     // Waiting to be published
    PROCESSING,  // Currently being published
    PROCESSED,   // Successfully published to Kafka
    FAILED       // Exhausted all retries
}
