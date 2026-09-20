package com.riskmonitor.risk_monitor_service.streams;

import com.riskmonitor.risk_monitor_service.model.TradeOrderEvent;
import com.riskmonitor.risk_monitor_service.service.RiskMonitorService;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.config.KafkaStreamsConfiguration;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.common.serialization.Serdes;
import org.springframework.kafka.support.serializer.JsonSerde;

@Configuration
@EnableKafkaStreams
public class RiskStreamsConfig {

    private final RiskMonitorService riskMonitorService;

    public RiskStreamsConfig(RiskMonitorService riskMonitorService) {
        this.riskMonitorService = riskMonitorService;
    }

@Bean(name = "defaultKafkaStreamsConfig")
public KafkaStreamsConfiguration kStreamsConfig(
        @Value("${spring.kafka.bootstrap-servers}") String bootstrapServers,
        @Value("${spring.kafka.streams.application-id}") String applicationId) {

    Map<String, Object> props = new HashMap<>();
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
    props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, JsonSerde.class);
    return new KafkaStreamsConfiguration(props);
}

    // =========================================================
    // RULE 1 - POSITION LIMIT
    // =========================================================

    @Bean
    public KStream<String, TradeOrderEvent> positionRiskStream(
            StreamsBuilder builder) {

        KStream<String, TradeOrderEvent> orders =
                builder.stream("trade-orders");

        KTable<String, Long> positions = orders

                // Only executed orders affect the position
                .filter((key, order) ->
                        order.getStatus().equals("EXECUTED")
                )

                // Example:
                // account123:BTC
                .selectKey((key, order) ->
                        order.getAccountId()
                                + ":"
                                + order.getAsset()
                )

                .groupByKey()

                // Current quantity for each account + asset
                .aggregate(
                        () -> 0L,

                        (key, order, currentQuantity) -> {

                            long orderQuantity =
                                    order.getQuantity().longValue();

                            if (order.getSide().equals("BUY")) {
                                return currentQuantity + orderQuantity;
                            }

                            if (order.getSide().equals("SELL")) {
                                return currentQuantity - orderQuantity;
                            }

                            return currentQuantity;
                        }
                );


        positions
                .toStream()

                .filter((accountAsset, quantity) -> {

                    // Temporary placeholder.
                    // Later this should come from market-price data.
                    long currentPrice = 100_000L;

                    long totalExposure =
                            quantity * currentPrice;

                    return riskMonitorService
                            .violatesPositionLimitThreshold(
                                    totalExposure
                            );
                })

                .to("flagged-orders");


        return orders;
    }


    // =========================================================
    // RULE 2 - CONCENTRATION LIMIT
    // =========================================================

    @Bean
    public KStream<String, TradeOrderEvent> concentrationRiskStream(
            StreamsBuilder builder) {

        KStream<String, TradeOrderEvent> orders =
                builder.stream("trade-orders");


        // -----------------------------------------------------
        // Value of each individual asset per account
        //
        // Example:
        // account123:BTC  -> 400000
        // account123:AAPL -> 50000
        // -----------------------------------------------------

        KTable<String, Long> assetValues = orders

                .filter((key, order) ->
                        order.getStatus().equals("EXECUTED")
                )

                .selectKey((key, order) ->
                        order.getAccountId()
                                + ":"
                                + order.getAsset()
                )

                .groupByKey()

                .aggregate(
                        () -> 0L,

                        (key, order, currentAssetValue) -> {

                            long quantity =
                                    order.getQuantity().longValue();

                            long price =
                                    order.getPrice().longValue();

                            long orderValue =
                                    quantity * price;

                            if (order.getSide().equals("BUY")) {
                                return currentAssetValue + orderValue;
                            }

                            if (order.getSide().equals("SELL")) {
                                return currentAssetValue - orderValue;
                            }

                            return currentAssetValue;
                        }
                );


        // -----------------------------------------------------
        // Total portfolio value per account
        //
        // Example:
        // account123 -> 450000
        // -----------------------------------------------------

        KTable<String, Long> totalPortfolioValues = orders

                .filter((key, order) ->
                        order.getStatus().equals("EXECUTED")
                )

                .selectKey((key, order) ->
                        order.getAccountId()
                )

                .groupByKey()

                .aggregate(
                        () -> 0L,

                        (accountId, order, currentPortfolioValue) -> {

                            long quantity =
                                    order.getQuantity().longValue();

                            long price =
                                    order.getPrice().longValue();

                            long orderValue =
                                    quantity * price;

                            if (order.getSide().equals("BUY")) {
                                return currentPortfolioValue + orderValue;
                            }

                            if (order.getSide().equals("SELL")) {
                                return currentPortfolioValue - orderValue;
                            }

                            return currentPortfolioValue;
                        }
                );


        // -----------------------------------------------------
        // Compare:
        //
        // asset value / total portfolio value
        //
        // Keep only concentration violations
        // -----------------------------------------------------

        assetValues
                .toStream()

                // account123:BTC -> account123
                .selectKey((accountAsset, assetValue) ->
                        accountAsset.split(":")[0] // Changed this so we can join the 2 tables on accountId
                )
                // Join because Rule 2 needs two different pieces of data at the same time
                .join(
                        totalPortfolioValues,

                        (assetValue, totalPortfolioValue) ->
                                riskMonitorService
                                        .violatesConcentrationLimit(
                                                assetValue,
                                                totalPortfolioValue
                                        )
                )

                .filter((accountId, violated) ->
                        violated
                )

                .to("flagged-orders");


        return orders;
    }


    // =========================================================
    // RULE 3 - RAPID ORDERS
    // =========================================================

        @Bean
    public KStream<String, TradeOrderEvent> rapidOrdersRiskStream(
            StreamsBuilder builder) {

        KStream<String, TradeOrderEvent> orders =
                builder.stream("trade-orders");

        KTable<Windowed<String>, Long> ordersPerAccountAndAssetPerWindow =
                orders
                        .filter((key, order) ->
                                order.getStatus().equals("EXECUTED")
                        )
                        .selectKey((key, order) ->
                                order.getAccountId()
                                        + ":"
                                        + order.getAsset()
                        )
                        .groupByKey()
                        .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(1)))
                        .count();

        ordersPerAccountAndAssetPerWindow
                        .toStream()
                        .filter((key, value) -> {

                            return riskMonitorService
                                .violatesRapidOrdersLimit(value);
                            
                        })
                        .to("flagged-orders");

        return orders;
    }
}