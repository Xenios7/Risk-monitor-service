package com.riskmonitor.risk_monitor_service.streams;

import com.riskmonitor.risk_monitor_service.model.FlaggedOrderPayload;
import com.riskmonitor.risk_monitor_service.model.TradeOrderEvent;
import com.riskmonitor.risk_monitor_service.service.RiskMonitorService;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
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
import org.springframework.kafka.support.serializer.JsonSerde;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

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

    // Bundles a running quantity with the last order that updated it,
    // so when a threshold is crossed we know which order to emit.
    private record QuantityState(BigDecimal quantity, TradeOrderEvent lastOrder) {}

    // Same idea, for dollar-value aggregates (concentration rule)
    private record ValueState(BigDecimal value, TradeOrderEvent lastOrder) {}

    // Same idea, for a windowed order count (rapid orders rule)
    private record CountState(long count, TradeOrderEvent lastOrder) {}

        // A small holder for sum + count, so we can compute the average,
        // plus the order that just updated it
    private record PriceStats(BigDecimal sum, long count, TradeOrderEvent lastOrder) {
        BigDecimal average() {
        if (count == 0) return BigDecimal.ZERO;
        return sum.divide(BigDecimal.valueOf(count), 10, RoundingMode.HALF_UP);
        }
    }

    // =========================================================
    // RULE 1 - POSITION LIMIT
    // =========================================================

    @Bean
    public KStream<String, TradeOrderEvent> positionRiskStream(
            StreamsBuilder builder) {

        KStream<String, TradeOrderEvent> orders =
                builder.stream("trade-orders");

        KTable<String, QuantityState> positions = orders

                // Only executed orders affect the position
                .filter((key, order) ->
                        order.getStatus().equals("FILLED")
                )

                // Example:
                // account123:BTC
                .selectKey((key, order) ->
                        order.getAccountId()
                                + ":"
                                + order.getAsset()
                )

                .groupByKey()

                // Current quantity for each account + asset,
                // plus the order that just updated it
                .aggregate(
                        () -> new QuantityState(BigDecimal.ZERO, null),

                        (key, order, current) -> {

                            BigDecimal orderQuantity = order.getQuantity();
                            BigDecimal newQuantity;

                            if (order.getSide().equals("buy")) {
                                newQuantity = current.quantity().add(orderQuantity);
                            } else if (order.getSide().equals("sell")) {
                                newQuantity = current.quantity().subtract(orderQuantity);
                            } else {
                                newQuantity = current.quantity();
                            }

                            return new QuantityState(newQuantity, order);
                        }
                );


        positions
                .toStream()

                .filter((accountAsset, state) -> {

                    // Temporary placeholder.
                    // Later this should come from market-price data.
                    BigDecimal currentPrice = BigDecimal.valueOf(100_000);

                    BigDecimal totalExposure =
                            state.quantity().multiply(currentPrice);

                    return riskMonitorService
                            .violatesPositionLimitThreshold(
                                    totalExposure
                            );
                })

                // Emit the real order that caused the violation (convert my internal working data into the actual message I want the rest of the system to see.)
                .map((accountAsset, state) -> {
                    FlaggedOrderPayload payload = FlaggedOrderPayload.builder()
                            .order(state.lastOrder())
                            .ruleType("POSITION_LIMIT")
                            .build();

                    return KeyValue.pair(accountAsset, payload); //creates the actual key-value pair that gets sent out
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

        KTable<String, ValueState> assetValues = orders

                .filter((key, order) ->
                        order.getStatus().equals("FILLED")
                )

                .selectKey((key, order) ->
                        order.getAccountId()
                                + ":"
                                + order.getAsset()
                )

                .groupByKey()

                .aggregate(
                        () -> new ValueState(BigDecimal.ZERO, null),

                        (key, order, current) -> {

                            // Calc the price
                            BigDecimal orderValue =
                                    order.getQuantity().multiply(order.getPrice());
                            BigDecimal newValue;

                            if (order.getSide().equals("buy")) {
                                newValue = current.value().add(orderValue);
                            } else if (order.getSide().equals("sell")) {
                                newValue = current.value().subtract(orderValue);
                            } else {
                                newValue = current.value();
                            }

                            return new ValueState(newValue, order);
                        }
                );


        // -----------------------------------------------------
        // Total portfolio value per account
        //
        // Example:
        // account123 -> 450000
        // -----------------------------------------------------

        KTable<String, BigDecimal> totalPortfolioValues = orders

                .filter((key, order) ->
                        order.getStatus().equals("FILLED")
                )

                .selectKey((key, order) ->
                        order.getAccountId()
                )

                .groupByKey()

                .aggregate(
                        () -> BigDecimal.ZERO,

                        (accountId, order, currentPortfolioValue) -> {

                            BigDecimal orderValue =
                                    order.getQuantity().multiply(order.getPrice());

                            if (order.getSide().equals("buy")) {
                                return currentPortfolioValue.add(orderValue);
                            }

                            if (order.getSide().equals("sell")) {
                                return currentPortfolioValue.subtract(orderValue);
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
                .selectKey((accountAsset, state) ->
                        accountAsset.split(":")[0] // Changed this so we can join the 2 tables on accountId
                )
                // Join because Rule 2 needs two different pieces of data at the same time
                .join(
                        totalPortfolioValues,

                        (state, totalPortfolioValue) -> {
                            if (riskMonitorService.violatesConcentrationLimit(
                                    state.value(), totalPortfolioValue)) {
                                return FlaggedOrderPayload.builder()
                                        .order(state.lastOrder())
                                        .ruleType("CONCENTRATION_LIMIT")
                                        .build();
                            }
                            return null;
                        }
                )

                .filter((accountId, payload) -> payload != null)

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

        KTable<Windowed<String>, CountState> ordersPerAccountAndAssetPerWindow =
                orders
                        .filter((key, order) ->
                                order.getStatus().equals("FILLED")
                        )
                        .selectKey((key, order) ->
                                order.getAccountId()
                                        + ":"
                                        + order.getAsset()
                        )
                        .groupByKey()
                        .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(1)))
                        // Count orders in the window, but also remember
                        // the last order so we can emit it if it violates
                        .aggregate(
                                () -> new CountState(0L, null),
                                (key, order, current) ->
                                        new CountState(current.count() + 1, order)
                        );
        ordersPerAccountAndAssetPerWindow
                        .toStream()
                        .filter((windowedKey, state) ->
                                riskMonitorService.violatesRapidOrdersLimit(state.count())
                        )
                        .map((windowedKey, state) -> {
                            FlaggedOrderPayload payload = FlaggedOrderPayload.builder()
                                    .order(state.lastOrder()) //Which order violated
                                    .ruleType("RAPID_ORDERS")
                                    .build();

                            return KeyValue.pair(windowedKey.key(), payload);
                        })
                        .to("flagged-orders");

        return orders;
    }


    // =========================================================
    // RULE 4 - Price deviation
    // =========================================================

    @Bean 
    public KStream<String, TradeOrderEvent> priceDeviationStream(
            StreamsBuilder builder) {

                KStream<String, TradeOrderEvent> orders = 
                        builder.stream("trade-orders");

                //Calc recent average for the asset, per time window
                KTable<Windowed<String>, PriceStats> avgPricePerAsset = orders
                        .filter((key, order) -> order.getStatus().equals("FILLED"))
                        .selectKey((key, order) -> order.getAsset())
                        .groupByKey()
                        .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(5)))
                        .aggregate(
                                () -> new PriceStats(BigDecimal.ZERO, 0L, null),
                                (key, order, current) -> new PriceStats(
                                        current.sum().add(order.getPrice()),
                                        current.count() + 1,
                                        order
                                )
                        );

                avgPricePerAsset
                        .toStream()
                        .filter((windowedKey, state) -> {
                                return riskMonitorService.violatesPriceDeviationLimit(state.average(), state.lastOrder().getPrice());
                        })
                        .map((windowedKey, state) -> {
                        FlaggedOrderPayload payload = FlaggedOrderPayload.builder()
                                .order(state.lastOrder())
                                .ruleType("PRICE_DEVIATION")
                                .build();
                        return KeyValue.pair(windowedKey.key(), payload);
                        })
                        .to("flagged-orders");
                                        
                return orders;
            }  

}