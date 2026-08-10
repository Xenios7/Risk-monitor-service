package com.riskmonitor.risk_monitor_service.model;

import java.math.BigDecimal;
import java.time.Instant;


import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.ManyToOne;
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
@Table(name = "flagged_order")
public class FlaggedOrder {


    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @NotNull
    @Column(name = "order_id", nullable = false)   
    private String orderId;

    @NotNull
    @ManyToOne(optional = false)
    @JoinColumn(name = "rule_id")
    private RiskRule riskRule;

    @NotNull
    @Column(name = "asset", nullable = false)   
    private String asset;

    @NotNull
    @Column(name = "side", nullable = false)  
    private String side;

    @NotNull
    @Column(name = "quantity", nullable = false)  
    private BigDecimal quantity;

    @NotNull
    @Column(name = "price", nullable = false)
    private BigDecimal price;

    @NotNull
    @Column(name = "created_at", nullable = false)
    private Instant createdAt;

    @NotNull
    @Column(name = "executed_at", nullable = true)
    private Instant executedAt;

}
