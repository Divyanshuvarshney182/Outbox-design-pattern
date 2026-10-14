package org.example.events;


import lombok.*;
import java.math.BigDecimal;
import java.time.LocalDateTime;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class OrderCreatedEvent {

    private String eventId;
    private String orderId;
    private String customerId;
    private String productId;
    private String productName;
    private Integer quantity;
    private BigDecimal totalAmount;
    private LocalDateTime createdAt;
}