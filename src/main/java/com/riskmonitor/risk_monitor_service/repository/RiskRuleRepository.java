package com.riskmonitor.risk_monitor_service.repository;

import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;

import com.riskmonitor.risk_monitor_service.model.RiskRule;

public interface RiskRuleRepository extends JpaRepository<RiskRule, Long>{

    List<RiskRule> findByRuleType(String ruleType);

    List<RiskRule> findByEnabledTrue();
    
} 