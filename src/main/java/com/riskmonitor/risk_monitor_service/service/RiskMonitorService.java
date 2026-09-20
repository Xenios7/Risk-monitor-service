package com.riskmonitor.risk_monitor_service.service;

import org.springframework.stereotype.Service;

@Service
public class RiskMonitorService {

    private static final long POSITION_LIMIT = 500_000L;
    private static final double CONCENTRATION_LIMIT = 0.80;
    private static final long RAPID_LIMIT = 10;


    public boolean violatesPositionLimitThreshold(long totalExposure) {
        return totalExposure > POSITION_LIMIT;
    }

    public boolean violatesConcentrationLimit(
            long assetValue,
            long totalPortfolioValue) {

        if (totalPortfolioValue <= 0) {
            return false;
        }

        double concentration =
                (double) assetValue / totalPortfolioValue;

        return concentration > CONCENTRATION_LIMIT;
    }

    public boolean violatesRapidOrdersLimit(long numOrders) {

        return numOrders > RAPID_LIMIT;
    }

}