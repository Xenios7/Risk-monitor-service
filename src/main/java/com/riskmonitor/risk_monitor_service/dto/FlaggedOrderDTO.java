package com.riskmonitor.risk_monitor_service.dto;

import java.math.BigDecimal;
import java.time.Instant;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class FlaggedOrderDTO {

    private Long id;
    private String orderId;
    private String ruleType;   // "POSITION_LIMIT" e.g
    private String asset;
    private String side;
    private BigDecimal quantity;
    private BigDecimal price;
    private Instant createdAt;
    private Instant executedAt;
}