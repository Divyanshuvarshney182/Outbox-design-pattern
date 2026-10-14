package com.sheemab.inventory_service.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Positive;
import lombok.Data;


@Data
public class StockRequest {
    @NotBlank(message = "productId is required")
    private String productId;

    @NotBlank(message = "productName is required")
    private String productName;

    @Positive(message = "quantity must be > 0")
    private int quantity;
}
