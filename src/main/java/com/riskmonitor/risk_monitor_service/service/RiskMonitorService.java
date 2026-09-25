package com.riskmonitor.risk_monitor_service.service;

import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.math.RoundingMode;

@Service
public class RiskMonitorService {

    private static final BigDecimal POSITION_LIMIT = BigDecimal.valueOf(500_000);
    private static final BigDecimal CONCENTRATION_LIMIT = BigDecimal.valueOf(0.80);
    private static final long RAPID_LIMIT = 10;


    public boolean violatesPositionLimitThreshold(BigDecimal totalExposure) {
        return totalExposure.compareTo(POSITION_LIMIT) > 0;
    }

    public boolean violatesConcentrationLimit(
            BigDecimal assetValue,
            BigDecimal totalPortfolioValue) {

        if (totalPortfolioValue.compareTo(BigDecimal.ZERO) <= 0) {
            return false;
        }

        BigDecimal concentration = assetValue.divide(totalPortfolioValue, 10, java.math.RoundingMode.HALF_UP);

        return concentration.compareTo(CONCENTRATION_LIMIT) > 0;
    }

    public boolean violatesRapidOrdersLimit(long numOrders) {

        return numOrders > RAPID_LIMIT;
    }

    public boolean violatesPriceDeviationLimit(BigDecimal average, BigDecimal orderPrice){
    if (average.compareTo(BigDecimal.ZERO) <= 0) {
        return false;
    }

    BigDecimal deviation = orderPrice
            .subtract(average)
            .abs()
            .divide(average, 10, RoundingMode.HALF_UP);

        return deviation.compareTo(BigDecimal.valueOf(0.05)) > 0; // more than 5% off
    }   

    public BigDecimal getPositionLimitThreshold() {
        return POSITION_LIMIT;
    }

    public BigDecimal getRapidOrdersLimit() {
        return BigDecimal.valueOf(RAPID_LIMIT);
    }
    

}