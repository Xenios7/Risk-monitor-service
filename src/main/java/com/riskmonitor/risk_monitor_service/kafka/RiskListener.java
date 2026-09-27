package com.riskmonitor.risk_monitor_service.kafka;

import com.riskmonitor.risk_monitor_service.model.FlaggedOrderPayload;
import com.riskmonitor.risk_monitor_service.service.FlaggedOrderService;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class RiskListener {
    private final FlaggedOrderService flaggedOrderService;

    public RiskListener(FlaggedOrderService flaggedOrderService) {
        this.flaggedOrderService = flaggedOrderService;
    }  

    // Auto-subscribes to flagged-orders; Spring/Jackson deserializes each message
    // into FlaggedOrderPayload (matching JSON fields to order/ruleType) and calls
    // this method automatically.
    @KafkaListener(topics = "flagged-orders", groupId = "risk-monitor-flagged-order-consumer")
    public void handleFlaggedOrder(FlaggedOrderPayload payload) {
        flaggedOrderService.handleFlaggedOrder(payload);
    }






}




















