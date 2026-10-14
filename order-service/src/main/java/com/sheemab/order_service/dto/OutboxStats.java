package com.sheemab.order_service.dto;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class OutboxStats {
    private long pending;
    private long processing;
    private long processed;
    private long failed;
}
