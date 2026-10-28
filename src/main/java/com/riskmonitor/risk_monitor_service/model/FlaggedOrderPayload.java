package com.riskmonitor.risk_monitor_service.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import tools.jackson.databind.annotation.JsonDeserialize;
import tools.jackson.databind.annotation.JsonPOJOBuilder;
import lombok.Builder;
import lombok.Value;

// What actually gets sent to flagged-orders: the real order that
// violated a rule, plus which rule it violated. No invented data.
@Value
@Builder
@JsonDeserialize(builder = FlaggedOrderPayload.FlaggedOrderPayloadBuilder.class)
public class FlaggedOrderPayload {

    // The actual order that triggered the violation
    TradeOrderEvent order;

    // Which rule was violated: POSITION_LIMIT, CONCENTRATION_LIMIT, RAPID_ORDERS
    @JsonProperty("rule_type")
    String ruleType;

    // Lombok fills in the builder methods; this only tells Jackson they have no "with" prefix
    @JsonPOJOBuilder(withPrefix = "")
    public static class FlaggedOrderPayloadBuilder {
    }
}