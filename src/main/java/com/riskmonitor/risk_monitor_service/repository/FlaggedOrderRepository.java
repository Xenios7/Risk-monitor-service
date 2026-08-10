package com.riskmonitor.risk_monitor_service.repository;

import org.springframework.data.jpa.repository.JpaRepository;

import com.riskmonitor.risk_monitor_service.model.FlaggedOrder;

import java.time.Instant;
import java.util.List;


public interface FlaggedOrderRepository extends JpaRepository<FlaggedOrder, Long>{

    List<FlaggedOrder> findByAsset(String asset);

    List<FlaggedOrder> findByRiskRule_RuleType(String ruleType);

    List<FlaggedOrder> findByCreatedAtBetween(Instant start, Instant end);
}