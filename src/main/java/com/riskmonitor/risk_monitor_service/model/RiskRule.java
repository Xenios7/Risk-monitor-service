package com.riskmonitor.risk_monitor_service.model;

import java.math.BigDecimal;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jakarta.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode
@Entity
@Table(name = "risk_rule")
public class RiskRule {
    
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @NotNull 
    @Column(name = "rule_type", nullable = false)   
    private String ruleType;

    @Column(name = "description")
    private String description;

    @NotNull
    @Column(name = "threshold", nullable = false)   
    private BigDecimal threshold;

    @NotNull
    @Column(name = "enabled", nullable = false)   
    private boolean enabled;

}
