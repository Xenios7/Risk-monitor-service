package com.riskmonitor.risk_monitor_service.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import tools.jackson.databind.annotation.JsonDeserialize;
import lombok.Builder;
import lombok.Value;

import java.math.BigDecimal;
import java.time.Instant;

@Value
@Builder
@JsonDeserialize(builder = TradeOrderEvent.TradeOrderEventBuilder.class)
public class TradeOrderEvent {

    String id;
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
















