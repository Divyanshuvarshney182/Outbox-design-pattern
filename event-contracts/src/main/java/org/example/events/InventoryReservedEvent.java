package org.example.events;


import lombok.*;
import java.time.LocalDateTime;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class InventoryReservedEvent {

    private String eventId;
    private String reservationId;
    private String orderId;
    private String productId;
    private Integer quantity;
    private boolean success;
    private String reason;
    private LocalDateTime reservedAt;
}