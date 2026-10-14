package com.sheemab.order_service.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import lombok.Data;

import java.math.BigDecimal;

@Data
public class CreateOrderRequest {

    @NotBlank(message = "customerId is required")
    private String customerId;

    @NotBlank(message = "productId is required")
    private String productId;

    @NotBlank(message = "productName is required")
    private String productName;

    @NotNull
    @Positive(message = "quantity must be > 0")
    private Integer quantity;

    @NotNull @Positive(message = "totalAmount must be > 0")
    private BigDecimal totalAmount;
}