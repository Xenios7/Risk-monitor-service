package com.riskmonitor.risk_monitor_service.model;

import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.time.Instant;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class TradeOrderEvent {

    String id;

    @JsonProperty("account_id")
    String accountId;

    String status;

    @JsonProperty("created_at")
    Instant createdAt;

    @JsonProperty("executed_at")
    Instant executedAt;

    String asset;

    String side;

    BigDecimal quantity;

    BigDecimal price;
}