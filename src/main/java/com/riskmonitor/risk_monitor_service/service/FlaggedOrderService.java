package com.riskmonitor.risk_monitor_service.service;

import com.riskmonitor.risk_monitor_service.repository.RiskRuleRepository;

import java.util.List;
import org.springframework.stereotype.Service;

import com.riskmonitor.risk_monitor_service.repository.FlaggedOrderRepository;
import com.riskmonitor.risk_monitor_service.model.FlaggedOrder;
import com.riskmonitor.risk_monitor_service.model.FlaggedOrderPayload;
import com.riskmonitor.risk_monitor_service.model.RiskRule;

@Service
public class FlaggedOrderService {
    private final RiskRuleRepository riskRuleRepository;
    private final FlaggedOrderRepository flaggedOrderRepository;

    public FlaggedOrderService(FlaggedOrderRepository flaggedOrderRepository, RiskRuleRepository riskRuleRepository){
        this.flaggedOrderRepository = flaggedOrderRepository;
        this.riskRuleRepository = riskRuleRepository;
    }    


    public void handleFlaggedOrder(FlaggedOrderPayload payload){


        FlaggedOrder flaggedOrder = new FlaggedOrder();

        //Order id
        flaggedOrder.setOrderId(payload.getOrder().getId());

        //Risk rule 
        List<RiskRule> matchingRules = riskRuleRepository.findByRuleType(payload.getRuleType());
        if (matchingRules.isEmpty()) { 
            throw new IllegalStateException("No RiskRule found for type: " + payload.getRuleType());
        }
        RiskRule riskRule = matchingRules.get(0);
        flaggedOrder.setRiskRule(riskRule);
        
        //Other fields
        flaggedOrder.setAsset(payload.getOrder().getAsset());
        flaggedOrder.setSide(payload.getOrder().getSide());
        flaggedOrder.setQuantity(payload.getOrder().getQuantity());
        flaggedOrder.setPrice(payload.getOrder().getPrice());
        flaggedOrder.setCreatedAt(payload.getOrder().getCreatedAt());
        flaggedOrder.setExecutedAt(payload.getOrder().getExecutedAt());        

        //save
        flaggedOrderRepository.save(flaggedOrder);

    }
}

