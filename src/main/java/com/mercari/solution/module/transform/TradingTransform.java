package com.mercari.solution.module.transform;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.mercari.solution.config.TransformConfig;
import com.mercari.solution.module.DataType;
import com.mercari.solution.module.FCollection;
import com.mercari.solution.util.pipeline.OptionUtil;
import com.mercari.solution.util.domain.finance.trading.exchange.Order;
import com.mercari.solution.util.domain.finance.trading.exchange.Position;
import com.mercari.solution.util.domain.finance.trading.exchange.Rule;
import com.mercari.solution.util.domain.finance.trading.exchange.Side;
import com.mercari.solution.util.domain.finance.trading.market.Market;
import com.mercari.solution.util.pipeline.union.Union;
import com.mercari.solution.util.pipeline.union.UnionCoder;
import com.mercari.solution.util.pipeline.union.UnionValue;
import com.mercari.solution.util.schema.AvroSchemaUtil;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.*;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.AfterPane;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.*;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

public class TradingTransform {

    private static final Logger LOG = LoggerFactory.getLogger(TradingTransform.class);


    private static class TradingTransformParameters implements Serializable {

        private JsonArray rules;

        private List<TradingMarket> markets;
        private List<Simulation> simulations;
        private List<String> portfolioFields;


        public JsonArray getRules() {
            return rules;
        }

        public List<TradingMarket> getMarkets() {
            return markets;
        }

        public List<Simulation> getSimulations() {
            return simulations;
        }

        public List<String> getPortfolioFields() {
            return portfolioFields;
        }

        private void validate() {
            final List<String> errorMessages = new ArrayList<>();
            if (rules == null || !rules.isJsonArray()) {
                errorMessages.add("Trading transform module requires rules parameter");
            } else {
                for(int i=0; i<rules.size(); i++) {
                    final JsonElement ruleElement = rules.get(i);
                    if(!ruleElement.isJsonObject()) {
                        errorMessages.add("Trading transform module requires rules parameter");
                    } else {
                        final JsonObject ruleObject= ruleElement.getAsJsonObject();
                        if(ruleObject.has("")) {

                        }
                    }
                    //errorMessages.addAll(rules.get(i).validate(i));
                }
            }

            if (markets == null || markets.isEmpty()) {
                errorMessages.add("Trading transform module requires markets parameter");
            } else {
                for(final TradingMarket market : this.markets) {
                    errorMessages.addAll(market.validate());
                }
            }

            if (!errorMessages.isEmpty()) {
                throw new IllegalArgumentException(String.join(", ", errorMessages));
            }
        }

        private void setDefaults() {
            //for(final Rule rule : this.rules) {
            //    rule.setDefaults();
            //}
            if(this.portfolioFields == null) {
                this.portfolioFields = new ArrayList<>();
            }
        }
    }

    private static class TradingMarket implements Serializable {

        private String name;
        private String key;
        private String secretKey;

        public String getName() {
            return name;
        }

        public String getKey() {
            return key;
        }

        public String getSecretKey() {
            return secretKey;
        }

        private List<String> validate() {
            final List<String> errorMessages = new ArrayList<>();
            return errorMessages;
        }

        private void setDefaults() {
            //for(final Rule rule : this.rules) {
            //    rule.setDefaults();

        }
    }

    private static class Simulation implements Serializable {

        private String market;
        private Map<String, Double> wallet;
        private Map<String, String> marketPriceFields;
        private Map<String, String> marketLowPriceFields;
        private Map<String, String> marketHighPriceFields;

        private Map<String, Double> feeRates;

        public String getMarket() {
            return market;
        }

        public Map<String, Double> getWallet() {
            return wallet;
        }

        public Map<String, String> getMarketPriceFields() {
            return marketPriceFields;
        }

        public Map<String, String> getMarketLowPriceFields() {
            return marketLowPriceFields;
        }

        public Map<String, String> getMarketHighPriceFields() {
            return marketHighPriceFields;
        }

        public Map<String, Double> getFeeRates() {
            return feeRates;
        }

        private void validate() {
            final List<String> errorMessages = new ArrayList<>();
            if (!errorMessages.isEmpty()) {
                throw new IllegalArgumentException(String.join(", ", errorMessages));
            }
        }

        private void setDefaults() {
            //for(final Rule rule : this.rules) {
            //    rule.setDefaults();

        }

    }


    public String getName() {
        return "trading";
    }


    public Map<String, FCollection<?>> expand(final List<FCollection<?>> inputs, final TransformConfig config) {
        return transform(inputs.get(0).getCollection().getPipeline(), inputs, config);
    }

    public static Map<String, FCollection<?>> transform(final Pipeline pipeline, final List<FCollection<?>> inputs, final TransformConfig config) {
        final TradingTransformParameters parameters = new Gson().fromJson(config.getParameters(), TradingTransformParameters.class);
        parameters.validate();
        parameters.setDefaults();

        final List<TupleTag<?>> inputTags = new ArrayList<>();
        final List<DataType> inputTypes = new ArrayList<>();
        final List<String> inputNames = new ArrayList<>();
        final Map<String, Schema> inputSchemas = new HashMap<>();

        PCollectionTuple tuple = PCollectionTuple.empty(pipeline);
        for(final FCollection input : inputs) {
            final TupleTag<?> tag = new TupleTag<>(){};
            tuple = tuple.and(tag, input.getCollection());

            inputTags.add(tag);
            inputTypes.add(input.getDataType());
            inputNames.add(input.getName());
            inputSchemas.put(input.getName(), input.getAvroSchema());
        }

        final List<Rule> rules = Rule.of(parameters.getRules());

        final Schema portfolioSchema = createPortfolioSchema(inputSchemas, rules, parameters.getPortfolioFields());
        final Schema positionsSchema = Position.schema();

        final TupleTag<GenericRecord> outputPortfolioTag = new TupleTag<>() {};
        final TupleTag<GenericRecord> outputPositionTag = new TupleTag<>() {};

        final PCollectionTuple results = tuple.apply(config.getName(), new Transform(
                config.getName(), parameters, inputTags, inputTypes, inputNames, inputSchemas, portfolioSchema.toString(), parameters.getPortfolioFields(), outputPortfolioTag, outputPositionTag));

        final Map<String, FCollection<?>> outputs = new HashMap<>();
        outputs.put(config.getName(), FCollection.of(config.getName(), results.get(outputPortfolioTag).setCoder(AvroCoder.of(portfolioSchema)), DataType.AVRO, portfolioSchema));
        final String positionName = config.getName() + ".positions";
        outputs.put(positionName, FCollection.of(positionName, results.get(outputPositionTag).setCoder(AvroCoder.of(positionsSchema)), DataType.AVRO, positionsSchema));

        return outputs;
    }

    private static class Transform extends PTransform<PCollectionTuple, PCollectionTuple> {

        private final String name;
        private final String rulesJson;
        private final List<TradingMarket> markets;
        private final List<Simulation> simulations;
        private final List<TupleTag<?>> inputTags;
        private final List<DataType> inputTypes;
        private final List<String> inputNames;
        private final Map<String, String> inputSchemas;
        private final String portfolioSchemaString;
        private final List<String> portfolioFields;

        private final TupleTag<GenericRecord> outputPortfolioTag;
        private final TupleTag<GenericRecord> outputPositionTag;


        Transform(final String name,
                  final TradingTransformParameters parameters,
                  final List<TupleTag<?>> inputTags,
                  final List<DataType> inputTypes,
                  final List<String> inputNames,
                  final Map<String, Schema> inputSchemas,
                  final String portfolioSchemaString,
                  final List<String> portfolioFields,
                  final TupleTag<GenericRecord> outputPortfolioTag,
                  final TupleTag<GenericRecord> outputPositionTag) {

            this.name = name;
            this.rulesJson = parameters.getRules().toString();
            this.markets = parameters.getMarkets();
            this.simulations = parameters.getSimulations();
            this.inputTags = inputTags;
            this.inputTypes = inputTypes;
            this.inputNames = inputNames;
            this.inputSchemas = inputSchemas.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().toString()));
            this.portfolioSchemaString = portfolioSchemaString;
            this.portfolioFields = portfolioFields;
            this.outputPortfolioTag = outputPortfolioTag;
            this.outputPositionTag = outputPositionTag;
        }

        @Override
        public PCollectionTuple expand(PCollectionTuple inputs) {

            final PCollection<KV<String, UnionValue>> union = inputs
                    .apply("Union", Union.withKey(inputTags, inputTypes, new ArrayList<>(), inputNames))
                    .apply("WithTrigger", Window
                            .<KV<String,UnionValue>>into(new GlobalWindows())
                            .triggering(Repeatedly.forever(AfterPane.elementCountAtLeast(1)))
                            .withAllowedLateness(Duration.ZERO)
                            .discardingFiredPanes());
            final UnionCoder unionCoder = (UnionCoder)((KvCoder)union.getCoder()).getValueCoder();

            final String mode;
            final TradeDoFn tradeDoFn;
            if(OptionUtil.isStreaming(inputs)) {
                mode = "TradingMarket";
                tradeDoFn = new StreamingTradeDoFn(name, rulesJson, markets, simulations, inputNames, portfolioSchemaString, portfolioFields, outputPortfolioTag, outputPositionTag, unionCoder);
            } else {
                mode = "TradingSimulation";
                tradeDoFn = new BatchTradeDoFn(name, rulesJson, markets, simulations, inputNames, portfolioSchemaString, portfolioFields, outputPortfolioTag, outputPositionTag, unionCoder);
            }

            return union.apply(mode, ParDo
                    .of(tradeDoFn)
                    .withOutputTags(outputPortfolioTag, TupleTagList.of(outputPositionTag)));
        }


        private abstract static class TradeDoFn extends DoFn<KV<String, UnionValue>, GenericRecord> {

            protected static final String STATE_ID_INPUTS = "inputs";
            protected static final String STATE_ID_POSITIONS = "positions";
            protected static final String STATE_ID_WALLETS = "wallets";

            private final String name;
            private final String rulesJson;
            protected final Map<String, Simulation> simulations;

            private final List<String> inputNames;

            private final String portfolioSchemaString;
            private final List<String> portfolioFields;

            //
            private final Map<String, KV<String, String>> apiKeys;

            private final TupleTag<GenericRecord> outputPortfolioTag;
            private final TupleTag<GenericRecord> outputPositionTag;


            private transient List<Rule> rules;
            private transient Schema portfolioSchema;
            private transient Schema positionSchema;
            private transient Set<String> variables;

            protected transient Map<String, Market> markets;


            TradeDoFn(final String name,
                      final String rulesJson,
                      final List<TradingMarket> tradingMarkets,
                      final List<Simulation> simulations,
                      final List<String> inputNames,
                      final String portfolioSchemaString,
                      final List<String> portfolioFields,
                      final TupleTag<GenericRecord> outputPortfolioTag,
                      final TupleTag<GenericRecord> outputPositionTag) {

                this.name = name;
                this.rulesJson = rulesJson;
                this.simulations = simulations.stream().collect(Collectors.toMap(Simulation::getMarket, s -> s));
                this.inputNames = inputNames;
                this.portfolioSchemaString = portfolioSchemaString;
                this.portfolioFields = portfolioFields;

                this.apiKeys = tradingMarkets
                        .stream()
                        .collect(Collectors.toMap(
                                TradingMarket::getName,
                                m -> KV.of(m.getKey(),m.getSecretKey())));

                this.outputPortfolioTag = outputPortfolioTag;
                this.outputPositionTag = outputPositionTag;
            }

            protected void setup() {
                this.rules = new ArrayList<>();
                this.variables = new HashSet<>();

                final JsonArray rulesArray = new Gson().fromJson(rulesJson, JsonArray.class);
                for(final JsonElement ruleElement : rulesArray) {
                    final Rule rule = Rule.of(ruleElement);
                    rule.setup();
                    this.rules.add(rule);
                    this.variables.addAll(rule.getRequiredVariables());
                }

                this.markets = this.rules.stream()
                        .map(Rule::getMarket)
                        .distinct()
                        .collect(Collectors.toMap(
                                market -> market,
                                market -> Market.createMarket(market, apiKeys.get(market).getKey(), apiKeys.get(market).getValue(), true)));

                this.portfolioSchema = AvroSchemaUtil.convertSchema(portfolioSchemaString);
                this.positionSchema = Position.schema();
            }

            protected void teardown() {

            }

            protected void process(
                    final KV<String, UnionValue> element,
                    Instant timestamp,
                    final ValueState<Map<String, UnionValue>> inputsState,
                    final ValueState<List<Position>> positionsState,
                    final ValueState<Map<String, Map<String, Double>>> walletsState,
                    final MultiOutputReceiver outputReceiver) {

                final Map<String, UnionValue> inputs = Optional
                        .ofNullable(inputsState.read())
                        .orElseGet(HashMap::new);

                if(element == null) {
                    timestamp = Instant.now();
                    LOG.info("final process: " + timestamp);
                } else {
                    final UnionValue input = element.getValue();
                    final String source = inputNames.get(input.getIndex());
                    inputs.put(source, input);

                    inputsState.write(inputs);
                }

                if(inputs.size() < inputNames.size()) {
                    LOG.info("inputs not ready");
                    return;
                }

                final List<Position> positions = Optional
                        .ofNullable(positionsState.read())
                        .orElseGet(ArrayList::new);

                final Map<String, Map<String, Double>> wallets = Optional
                        .ofNullable(walletsState.read())
                        .orElseGet(() -> {
                            if(simulations == null) {
                                // TODO
                                return new HashMap<>();
                            } else {
                                return simulations.entrySet().stream().collect(Collectors.toMap(
                                        Map.Entry::getKey,
                                        e -> e.getValue().getWallet()));
                            }
                        });

                // Update existing positions state
                final KV<List<Position>, Map<String, Map<String, Double>>> updatedPositionsAndWallet = update(positions, wallets, inputs, timestamp);
                final List<Position> updatedPositions = new ArrayList<>();
                final List<Position> changedPositions = new ArrayList<>();
                // Output
                for(final Position position : updatedPositionsAndWallet.getKey()) {
                    if(position.getCompleted() && !Position.Target.entry.equals(position.getTarget())) {
                        final GenericRecord output = Position.toRecord(positionSchema, position);
                        outputReceiver.get(outputPositionTag).output(output);
                        changedPositions.add(position);
                    } else {
                        updatedPositions.add(position);
                    }
                }

                // Create orders from rules
                final List<Order> orders = new ArrayList<>();
                final Map<String, Object> ruleVariables = createRuleVariables(inputs, wallets, variables);
                for(final Rule rule : rules) {
                    final List<Order> newOrders = rule.apply(updatedPositions, updatedPositionsAndWallet.getValue(), ruleVariables, timestamp);
                    orders.addAll(newOrders);
                }
                final KV<List<Position>, Map<String, Map<String, Double>>> nextPositionsAndWallet = order(orders, updatedPositions, updatedPositionsAndWallet.getValue(), inputs, timestamp);

                // Update portfolio state
                walletsState.write(nextPositionsAndWallet.getValue());
                positionsState.write(nextPositionsAndWallet.getKey());

                // Generate output and send
                changedPositions.addAll(nextPositionsAndWallet.getKey().stream().filter(Position::getChanged).toList());
                final GenericRecord portfolio = createPortfolio(
                        name, portfolioSchema, portfolioFields,
                        inputs,
                        orders,
                        nextPositionsAndWallet.getKey(),
                        nextPositionsAndWallet.getValue(),
                        changedPositions,
                        timestamp);
                outputReceiver.get(outputPortfolioTag).output(portfolio);
            }

            public abstract KV<List<Position>, Map<String,Map<String,Double>>> update(
                    List<Position> positions,
                    Map<String, Map<String, Double>> wallets,
                    Map<String, UnionValue> inputs,
                    Instant timestamp);

            public abstract KV<List<Position>, Map<String, Map<String,Double>>> order(
                    List<Order> orders,
                    List<Position> positions,
                    Map<String, Map<String, Double>> wallets,
                    Map<String, UnionValue> inputs,
                    Instant timestamp);

            protected static Map<String, Object> createRuleVariables(
                    final Map<String, UnionValue> inputs,
                    final Map<String, Map<String, Double>> wallets,
                    final Set<String> variables) {

                final Map<String, Object> values = new HashMap<>();

                // inputs variables
                for(final Map.Entry<String, UnionValue> input : inputs.entrySet()) {
                    final Set<String> fieldsNames = variables.stream()
                            .map(v -> {
                                if(v.contains(".")) {
                                    final String[] strs = v.split("\\.");
                                    if(strs[0].equals(input.getKey())) {
                                        return strs[strs.length - 1];
                                    }
                                    return null;
                                } else {
                                    return v;
                                }
                            })
                            .filter(Objects::nonNull)
                            .collect(Collectors.toSet());
                    final Map<String, Object> inputFieldValues = input.getValue().getMap(fieldsNames);
                    for(final Map.Entry<String, Object> inputFieldValue : inputFieldValues.entrySet()) {
                        final String key = String.format("%s.%s", input.getKey(), inputFieldValue.getKey());
                        values.put(key, inputFieldValue.getValue());
                    }
                }

                // wallets variables
                for(final Map.Entry<String, Map<String, Double>> marketAndWallet : wallets.entrySet()) {
                    for(final Map.Entry<String, Double> currency : marketAndWallet.getValue().entrySet()) {
                        final String key = String.format("__wallets.%s.%s", marketAndWallet.getKey(), currency.getKey());
                        values.put(key, currency.getValue());
                    }
                }

                return values;
            }

            private static Map<String, Double> calculateValuation(List<Position> positions, Map<String, Map<String, Double>> wallets) {
                final Map<String, Double> total = new HashMap<>();
                for(final Map.Entry<String, Map<String, Double>> marketAndWallet : wallets.entrySet()) {
                    for(final Map.Entry<String, Double> currency : marketAndWallet.getValue().entrySet()) {
                        total.compute(currency.getKey(), (key, amount) -> Optional.ofNullable(amount).orElse(0D) + currency.getValue());
                    }
                }

                for(final Position position : positions) {
                    final double valuationAmount = switch (position.getTarget()) {
                        case entry, exit -> {
                            if(Side.buy.equals(position.getSide())) {
                                if(position.getCompleted()) {
                                    yield position.getContractPrice() * position.getSize();
                                } else if(position.getMarketPrice() != null) {
                                    yield position.getMarketPrice() * position.getSize();
                                } else {
                                    yield position.getRestrictedAmount();
                                }
                            } else {
                                if(position.getCompleted()) {
                                    yield 2 * position.getRestrictedAmount() - position.getContractPrice() * position.getSize();
                                } else if(position.getMarketPrice() != null) {
                                    yield 2 * position.getRestrictedAmount() - position.getMarketPrice() * position.getSize();
                                } else {
                                    yield position.getRestrictedAmount();
                                }
                            }
                        }
                        case cancel -> position.getRestrictedAmount();
                    };
                    total.compute(position.getCurrency(), (key, amount) -> Optional.ofNullable(amount).orElse(0D) + valuationAmount);
                }
                return total;
            }

            private static GenericRecord createPortfolio(
                    final String name,
                    final Schema schema,
                    final List<String> portfolioFields,
                    final Map<String, UnionValue> inputs,
                    final List<Order> orders,
                    final List<Position> positions,
                    final Map<String, Map<String,Double>> wallets,
                    final List<Position> updatedPositions,
                    final Instant timestamp) {

                final Schema walletSchema = schema.getField("wallets").schema().getElementType();
                final List<GenericRecord> walletRecords = wallets.entrySet().stream()
                        .flatMap(e -> e.getValue().entrySet().stream()
                                .map(ee -> new GenericRecordBuilder(walletSchema)
                                        .set("market", e.getKey())
                                        .set("currency", ee.getKey())
                                        .set("amount", ee.getValue())
                                        .build()))
                        .collect(Collectors.toList());

                final org.apache.avro.Schema positionSchema = schema.getField("positions").schema().getElementType();
                final List<GenericRecord> positionRecords = positions.stream()
                        .map(p -> Position.toRecord(positionSchema, p))
                        .collect(Collectors.toList());

                final Map<String, Double> valuationMap = calculateValuation(positions, wallets);
                final Schema valuationSchema = schema.getField("valuation").schema().getElementType();
                final List<GenericRecord> valuation = valuationMap.entrySet().stream()
                        .map(e -> new GenericRecordBuilder(valuationSchema)
                                .set("currency", e.getKey())
                                .set("amount", e.getValue())
                                .build())
                        .collect(Collectors.toList());

                GenericRecordBuilder builder = new GenericRecordBuilder(schema)
                        .set("name", name)
                        .set("wallets", walletRecords)
                        .set("positions", positionRecords)
                        .set("valuation", valuation)
                        .set("timestamp", timestamp.getMillis() * 1000L);

                for(final Map.Entry<String, Double> entry : valuationMap.entrySet()) {
                    builder.set("valuation_" + entry.getKey(), entry.getValue());
                }

                for(final Order order : orders) {
                    switch (order.getTarget()) {
                        case entry -> {
                            builder.set("rule_" + order.getRuleId() + "_entry_size", order.getSize());
                        }
                        case cancel -> {
                            builder.set("rule_" + order.getRuleId() + "_cancel_size", order.getSize());
                        }
                        case exit -> {
                            builder.set("rule_" + order.getRuleId() + "_exit_id", order.getExitId());
                            builder.set("rule_" + order.getRuleId() + "_exit_size", order.getSize());
                            builder.set("rule_" + order.getRuleId() + "_exit_" + order.getExitId() + "_size", order.getSize());
                        }
                    }
                }

                for(final Position updatedPosition : updatedPositions) {
                    switch (updatedPosition.getTarget()) {
                        case entry -> {
                            builder.set("rule_" + updatedPosition.getRuleId() + "_entry_completed_size", updatedPosition.getSize());
                        }
                        case cancel -> {
                            builder.set("rule_" + updatedPosition.getRuleId() + "_cancel_completed_size", updatedPosition.getSize());
                        }
                        case exit -> {
                            builder.set("rule_" + updatedPosition.getRuleId() + "_exit_completed_id", updatedPosition.getExitId());
                            builder.set("rule_" + updatedPosition.getRuleId() + "_exit_completed_size", updatedPosition.getSize());
                        }
                    }
                }

                for(final String portfolioField : portfolioFields) {
                    final String[] strs = portfolioField.split("\\.");
                    final String source = strs[0];
                    final String field  = strs[1];

                    final UnionValue input = inputs.get(source);
                    final Object value = UnionValue.getFieldValue(input, field);// input.getValue();//.get(field);
                    builder.set(field, value);
                }

                return builder.build();
            }

            private static String createLogText(final Map<String, Double> wallet, final List<Position> positions) {
                return String.format("portfolio:\n  - wallet: %s \n  - positions: [\n   %s\n    ]", wallet, positions.stream().map(Position::toString).collect(Collectors.joining("\n")));
            }

        }

        private static class BatchTradeDoFn extends TradeDoFn {

            @StateId(STATE_ID_INPUTS)
            private final StateSpec<@NonNull ValueState<Map<String,UnionValue>>> inputsStateSpec;

            @StateId(STATE_ID_POSITIONS)
            private final StateSpec<@NonNull ValueState<List<Position>>> positionsStateSpec;

            @StateId(STATE_ID_WALLETS)
            private final StateSpec<@NonNull ValueState<Map<String,Map<String,Double>>>> walletsStateSpec;


            BatchTradeDoFn(final String name,
                           final String rulesJson,
                           final List<TradingMarket> tradingMarkets,
                           final List<Simulation> simulations,
                           final List<String> inputNames,
                           final String portfolioSchemaString,
                           final List<String> portfolioFields,
                           final TupleTag<GenericRecord> outputPortfolioTag,
                           final TupleTag<GenericRecord> outputPositionTag,
                           final UnionCoder inputCoder) {

                super(name, rulesJson, tradingMarkets, simulations, inputNames, portfolioSchemaString, portfolioFields, outputPortfolioTag, outputPositionTag);

                this.inputsStateSpec = StateSpecs.value(MapCoder.of(StringUtf8Coder.of(), inputCoder));
                this.positionsStateSpec = StateSpecs.value(ListCoder.of(AvroCoder.of(Position.class)));
                this.walletsStateSpec = StateSpecs.value(MapCoder.of(StringUtf8Coder.of(), MapCoder.of(StringUtf8Coder.of(), DoubleCoder.of())));
            }

            @Setup
            public void setup() {
                super.setup();
            }

            @Teardown
            public void teardown() {
                super.teardown();
            }

            @ProcessElement
            @RequiresTimeSortedInput
            public void processElement(final @Element KV<String,UnionValue> element,
                                       final @Timestamp Instant timestamp,
                                       final MultiOutputReceiver outputReceiver,
                                       final @AlwaysFetched @StateId(STATE_ID_INPUTS) ValueState<Map<String,UnionValue>> inputsState,
                                       final @AlwaysFetched @StateId(STATE_ID_POSITIONS) ValueState<List<Position>> positionsState,
                                       final @AlwaysFetched @StateId(STATE_ID_WALLETS) ValueState<Map<String, Map<String, Double>>> walletsState) {

                super.process(element, timestamp, inputsState, positionsState, walletsState, outputReceiver);
            }

            @OnWindowExpiration
            public void onWindowExpiration(final @Timestamp Instant timestamp,
                                           final MultiOutputReceiver outputReceiver,
                                           final @AlwaysFetched @StateId(STATE_ID_INPUTS) ValueState<Map<String,UnionValue>> inputsState,
                                           final @AlwaysFetched @StateId(STATE_ID_POSITIONS) ValueState<List<Position>> positionsState,
                                           final @AlwaysFetched @StateId(STATE_ID_WALLETS) ValueState<Map<String, Map<String, Double>>> walletsState) {

                super.process(null, timestamp, inputsState, positionsState, walletsState, outputReceiver);
            }


            @Override
            public KV<List<Position>, Map<String,Map<String,Double>>> update(
                    final List<Position> positions,
                    final Map<String, Map<String, Double>> wallets,
                    final Map<String, UnionValue> inputs,
                    final Instant timestamp) {

                final List<Position> nextPositions = new ArrayList<>();
                final Map<String, Map<String, Double>> nextWallets = new HashMap<>(wallets);
                for(final Position prevPosition : positions) {

                    if(!simulations.containsKey(prevPosition.getMarket())) {
                        throw new IllegalArgumentException();
                    }
                    final Simulation simulation = simulations.get(prevPosition.getMarket());

                    final Double marketPrice = getPrice(prevPosition, simulation.getMarketPriceFields(), inputs);
                    final Double lowPrice = getPrice(prevPosition, simulation.getMarketLowPriceFields(), inputs);
                    final Double highPrice = getPrice(prevPosition, simulation.getMarketHighPriceFields(), inputs);
                    prevPosition.setMarketPrice(marketPrice);

                    final Position nextPosition = Position.copy(prevPosition);
                    if(!prevPosition.getCompleted()) {
                        if(Position.Target.cancel.equals(prevPosition.getTarget())) {
                            nextPosition.setCompleted(true);
                        } else if(!prevPosition.getLimit()) {
                            nextPosition.setCompleted(true);
                            nextPosition.setContractPrice(marketPrice);
                        } else {
                            if(lowPrice != null && highPrice != null) {
                                if((Position.Target.entry.equals(prevPosition.getTarget()) && ((Side.buy.equals(prevPosition.getSide()) && lowPrice < prevPosition.getEntryOrderPrice()) || (Side.sell.equals(prevPosition.getSide()) && highPrice > prevPosition.getEntryOrderPrice())))
                                        || (Position.Target.exit.equals(prevPosition.getTarget()) && ((Side.buy.equals(prevPosition.getSide()) && highPrice > prevPosition.getExitOrderPrice()) || (Side.sell.equals(prevPosition.getSide()) && lowPrice < prevPosition.getExitOrderPrice())))) {
                                    nextPosition.setCompleted(true);
                                    nextPosition.setContractPrice(prevPosition.getOrderPrice());
                                } else {
                                    nextPosition.setCompleted(false);
                                    nextPosition.setContractPrice(null);
                                }
                            } else {
                                if((Position.Target.entry.equals(prevPosition.getTarget()) && ((Side.buy.equals(prevPosition.getSide()) && marketPrice < prevPosition.getEntryOrderPrice()) || (Side.sell.equals(prevPosition.getSide()) && marketPrice > prevPosition.getEntryOrderPrice())))
                                        || (Position.Target.exit.equals(prevPosition.getTarget()) && ((Side.buy.equals(prevPosition.getSide()) && marketPrice > prevPosition.getExitOrderPrice()) || (Side.sell.equals(prevPosition.getSide()) && marketPrice < prevPosition.getExitOrderPrice())))) {
                                    nextPosition.setCompleted(true);
                                    nextPosition.setContractPrice(prevPosition.getOrderPrice());
                                } else {
                                    nextPosition.setCompleted(false);
                                    nextPosition.setContractPrice(null);
                                }
                            }

                        }

                        nextPosition.setChanged(nextPosition.getCompleted());
                    } else {
                        nextPosition.setChanged(!nextPosition.getCompleted());
                    }

                    if(nextPosition.getCompleted() && !Position.Target.cancel.equals(nextPosition.getTarget())) {
                        final Double feeAmount = this.markets.get(nextPosition.getMarket()).fee(
                                nextPosition.getContractPrice(),
                                nextPosition.getSize(),
                                nextPosition.getLimit(),
                                nextPosition.getSymbol());
                        if(feeAmount == null || feeAmount == 0) {
                            LOG.warn("feeAmount is zero: " + nextPosition.getContractPrice() + ", " + nextPosition.getSize() + ", " + nextPosition.getLimit());
                        }
                        nextPosition.setFeeAmount(feeAmount);
                    }

                    final List<Position> updatedPositions = new ArrayList<>();
                    updatedPositions.add(nextPosition);
                    for(final Position updatedPosition : updatedPositions) {
                        if(updatedPosition.getCompleted() && !prevPosition.getCompleted()) {
                            switch (updatedPosition.getTarget()) {
                                case entry -> applyCompleteEntry(nextWallets, updatedPosition);
                                case cancel -> applyCompleteCancel(nextWallets, updatedPosition);
                                case exit -> applyCompleteExit(nextWallets, updatedPosition);
                                default -> throw new IllegalArgumentException();
                            }
                        }
                    }

                    nextPositions.addAll(updatedPositions);
                }

                return KV.of(nextPositions, nextWallets);
            }

            @Override
            public KV<List<Position>, Map<String,Map<String,Double>>> order(
                    final List<Order> orders,
                    final List<Position> positions,
                    final Map<String, Map<String, Double>> wallets,
                    final Map<String, UnionValue> inputs,
                    final Instant timestamp) {

                if(orders.isEmpty()) {
                    return KV.of(positions, wallets);
                }

                final List<Position> nextPositions = new ArrayList<>();
                for(final Position prevPosition : positions) {
                    if(!hasRelatedOrder(orders, prevPosition.getOrderId())) {
                        nextPositions.add(prevPosition);
                    }
                }

                final Map<String, Map<String, Double>> nextWallets = new HashMap<>(wallets);
                for(final Order order : orders) {
                    final Position prevPosition = getPosition(positions, order.getOrderId());
                    final String orderId = UUID.randomUUID().toString();
                    final Position nextPosition = Position.of(order, prevPosition, orderId, timestamp);

                    if(!simulations.containsKey(order.getMarket())) {
                        throw new IllegalArgumentException();
                    }
                    final Simulation simulation = simulations.get(order.getMarket());

                    final Double marketPrice = getPrice(prevPosition, simulation.getMarketPriceFields(), inputs);
                    nextPosition.setMarketPrice(marketPrice);
                    applyOrder(nextWallets, nextPosition);
                    nextPositions.add(nextPosition);
                }

                return KV.of(nextPositions, nextWallets);
            }

            private static void applyOrder(final Map<String, Map<String, Double>> wallets, final Position position) {
                if(!Position.Target.entry.equals(position.getTarget())) {
                    return;
                }

                final Double restrictedAmount;
                if(position.getRestrictedAmount() != null) {
                    restrictedAmount = position.getRestrictedAmount();
                } else {
                    restrictedAmount = position.getSize() * Optional
                            .ofNullable(position.getEntryOrderPrice())
                            .orElseGet(position::getMarketPrice);
                    position.setRestrictedAmount(restrictedAmount);
                }
                wallets.get(position.getMarket()).compute(position.getCurrency(),
                        (currency, amount) -> Optional.ofNullable(amount).orElse(0D) - restrictedAmount);
            }

            private static void applyCompleteEntry(final Map<String, Map<String, Double>> wallets, final Position position) {
                LOG.info("content_wallet_entry0: " + wallets);
                wallets.get(position.getMarket()).compute(position.getCurrency(),
                        (currency, amount) -> Optional.ofNullable(amount).orElse(0D)
                                + position.getRestrictedAmount()
                                - position.getEntryContractPrice() * position.getSize()
                                - position.getEntryFeeAmount());
                LOG.info("content_wallet_entry1: " + wallets + ", position: " + position);
            }

            private static void applyCompleteCancel(final Map<String, Map<String, Double>> wallets, final Position position) {
                LOG.info("content_wallet_cancel0: " + wallets);
                wallets.get(position.getMarket()).compute(position.getCurrency(),
                        (currency, amount) -> Optional.ofNullable(amount).orElse(0D)
                                + position.getRestrictedAmount());
                LOG.info("content_wallet_cancel1: " + wallets + ", position: " + position);
            }

            private static void applyCompleteExit(final Map<String, Map<String, Double>> wallets, final Position position) {
                LOG.info("content_wallet_exit0: " + wallets);
                if(position.getExitContractPrice() == null) {
                    throw new IllegalArgumentException("applyCompleteExit error for position: " + position);
                }
                wallets.get(position.getMarket()).compute(position.getCurrency(),
                        (currency, amount) -> Optional.ofNullable(amount).orElse(0D)
                                + position.getExitContractPrice() * position.getSize()
                                - position.getExitFeeAmount());
                LOG.info("content_wallet_exit1: " + wallets + ", position: " + position);
            }

            private static Position getPosition(final List<Position> positions, final String orderId) {
                if(orderId == null || positions == null || positions.isEmpty()) {
                    return null;
                }
                return positions.stream()
                        .filter(p -> orderId.equals(p.getOrderId()))
                        .findAny()
                        .orElse(null);
            }



            private static Double getPrice(
                    final Position position,
                    final Map<String, String> priceFields,
                    final Map<String, UnionValue> inputs) {

                if(position == null || inputs == null) {
                    return null;
                }
                if(priceFields == null) {
                    throw new IllegalArgumentException("simulations must not be null for batch simulation mode");
                }
                if(!priceFields.containsKey(position.getSymbol())) {
                    throw new IllegalArgumentException("simulations does not contains priceField for market: " + position.getMarket() + ", symbol: " + position.getSymbol() + " in values: " + priceFields.get(position.getMarket()));
                }
                final String priceFieldName = priceFields.get(position.getSymbol());
                final String[] strs = priceFieldName.split("\\.");
                final String source = strs[0];
                final String field  = strs[1];
                if(!inputs.containsKey(source)) {
                    return null;
                }
                final UnionValue input = inputs.get(source);
                if(position.getCreated().getMillis() > input.getEpochMillis()) {
                    return null;
                } else {
                    return UnionValue.getAsDouble(input, field);
                }
            }

            private static boolean hasRelatedOrder(final List<Order> orders, final String orderId) {
                if(orderId == null || orders == null || orders.isEmpty()) {
                    return false;
                }
                return orders.stream()
                        .filter(o -> o.getOrderId() != null)
                        .anyMatch(o -> orderId.equals(o.getOrderId()));
            }

        }

        private static class StreamingTradeDoFn extends TradeDoFn {

            @StateId(STATE_ID_INPUTS)
            private final StateSpec<@NonNull ValueState<Map<String,UnionValue>>> inputsStateSpec;

            @StateId(STATE_ID_POSITIONS)
            private final StateSpec<@NonNull ValueState<List<Position>>> positionsStateSpec;

            @StateId(STATE_ID_WALLETS)
            private final StateSpec<@NonNull ValueState<Map<String,Map<String,Double>>>> walletsStateSpec;


            StreamingTradeDoFn(final String name,
                               final String rulesJson,
                               final List<TradingMarket> tradingMarkets,
                               final List<Simulation> simulations,
                               final List<String> inputNames,
                               final String portfolioSchemaString,
                               final List<String> portfolioFields,
                               final TupleTag<GenericRecord> outputPortfolioTag,
                               final TupleTag<GenericRecord> outputPositionTag,
                               final UnionCoder inputCoder) {

                super(name, rulesJson, tradingMarkets, simulations, inputNames, portfolioSchemaString, portfolioFields, outputPortfolioTag, outputPositionTag);

                this.inputsStateSpec = StateSpecs.value(MapCoder.of(StringUtf8Coder.of(), inputCoder));
                this.positionsStateSpec = StateSpecs.value(ListCoder.of(AvroCoder.of(Position.class)));
                this.walletsStateSpec = StateSpecs.value(MapCoder.of(StringUtf8Coder.of(), MapCoder.of(StringUtf8Coder.of(), DoubleCoder.of())));
            }


            @Setup
            public void setup() {
                super.setup();
            }

            @Teardown
            public void teardown() {
                super.teardown();
            }

            @ProcessElement
            public void processElement(final ProcessContext c,
                                       final MultiOutputReceiver outputReceiver,
                                       final @AlwaysFetched @StateId(STATE_ID_INPUTS) ValueState<Map<String,UnionValue>> inputsState,
                                       final @AlwaysFetched @StateId(STATE_ID_POSITIONS) ValueState<List<Position>> positionsState,
                                       final @AlwaysFetched @StateId(STATE_ID_WALLETS) ValueState<Map<String,Map<String, Double>>> walletsState) {

                super.process(c.element(), c.timestamp(), inputsState, positionsState, walletsState, outputReceiver);
            }

            @Override
            public KV<List<Position>, Map<String,Map<String,Double>>> update(
                    final List<Position> positions,
                    final Map<String,Map<String, Double>> wallets,
                    final Map<String, UnionValue> inputs,
                    final Instant timestamp) {

                final List<Position> nextPositions = new ArrayList<>();
                final Map<String, Map<String, Double>> nextWallets = new HashMap<>(wallets);
                for(final Position prevPosition : positions) {

                }

                return KV.of(nextPositions, nextWallets);
            }

            @Override
            public KV<List<Position>, Map<String,Map<String,Double>>> order(
                    final List<Order> orders,
                    final List<Position> positions,
                    final Map<String, Map<String, Double>> wallets,
                    final Map<String, UnionValue> inputs,
                    final Instant timestamp) {

                if(orders.isEmpty()) {
                    return KV.of(positions, wallets);
                }

                final Map<String,List<Order>> ordersMap = new HashMap<>();
                for(final Order order : orders) {
                    if(!ordersMap.containsKey(order.getMarket())) {
                        ordersMap.put(order.getMarket(), new ArrayList<>());
                    }
                    ordersMap.get(order.getMarket()).add(order);
                }

                final List<Position> nextPositions = new ArrayList<>();
                for(final Map.Entry<String, List<Order>> entry : ordersMap.entrySet()) {
                    final List<Position> results = markets.get(entry.getKey()).place(entry.getValue());
                    nextPositions.addAll(results);
                }

                final Map<String, Map<String, Double>> nextWallets = new HashMap<>(wallets);

                return KV.of(nextPositions, nextWallets);
            }

        }

    }

    private static Schema createPortfolioSchema(
            final Map<String, Schema> inputSchemas,
            final List<Rule> rules,
            final List<String> portfolioFields) {

        final SchemaBuilder.FieldAssembler<Schema> schemaFields = SchemaBuilder.record("Portfolio").fields()
                .name("name").type(Schema.createUnion(Schema.create(Schema.Type.STRING), Schema.create(Schema.Type.NULL))).noDefault()
                .name("wallets").type(Schema.createArray(createWalletSchema())).noDefault()
                .name("positions").type(Schema.createArray(Position.schema())).noDefault()
                .name("valuation").type(Schema.createArray(SchemaBuilder.builder().record("Valuation").fields()
                        .name("currency").type(Schema.create(Schema.Type.STRING)).noDefault()
                        .name("amount").type(Schema.create(Schema.Type.DOUBLE)).noDefault()
                        .endRecord())).noDefault()
                .name("timestamp").type(LogicalTypes.timestampMicros().addToSchema(Schema.create(Schema.Type.LONG))).noDefault();

        final List<String> currencies = rules.stream().map(Rule::getCurrency).distinct().toList();
        for(final String currency : currencies) {
            schemaFields.name("valuation_" + currency).type(Schema.createUnion(Schema.create(Schema.Type.DOUBLE), Schema.create(Schema.Type.NULL))).withDefault(0D);
        }

        for(final Rule rule : rules) {
            schemaFields.name("rule_" + rule.getRuleId() + "_entry_size").type(Schema.createUnion(Schema.create(Schema.Type.DOUBLE), Schema.create(Schema.Type.NULL))).withDefault(0D);
            schemaFields.name("rule_" + rule.getRuleId() + "_entry_completed_size").type(Schema.createUnion(Schema.create(Schema.Type.DOUBLE), Schema.create(Schema.Type.NULL))).withDefault(0D);
            schemaFields.name("rule_" + rule.getRuleId() + "_cancel_size").type(Schema.createUnion(Schema.create(Schema.Type.DOUBLE), Schema.create(Schema.Type.NULL))).withDefault(0D);
            schemaFields.name("rule_" + rule.getRuleId() + "_cancel_completed_size").type(Schema.createUnion(Schema.create(Schema.Type.DOUBLE), Schema.create(Schema.Type.NULL))).withDefault(0D);
            schemaFields.name("rule_" + rule.getRuleId() + "_exit_id").type(Schema.createUnion(Schema.create(Schema.Type.STRING), Schema.create(Schema.Type.NULL))).withDefault("");
            schemaFields.name("rule_" + rule.getRuleId() + "_exit_size").type(Schema.createUnion(Schema.create(Schema.Type.DOUBLE), Schema.create(Schema.Type.NULL))).withDefault(0D);
            schemaFields.name("rule_" + rule.getRuleId() + "_exit_completed_id").type(Schema.createUnion(Schema.create(Schema.Type.STRING), Schema.create(Schema.Type.NULL))).withDefault("");
            schemaFields.name("rule_" + rule.getRuleId() + "_exit_completed_size").type(Schema.createUnion(Schema.create(Schema.Type.DOUBLE), Schema.create(Schema.Type.NULL))).withDefault(0D);
            for(final Rule.Exit exit : rule.getExits()) {
                schemaFields.name("rule_" + rule.getRuleId() + "_exit_" + exit.getExitId() + "_size").type(Schema.createUnion(Schema.create(Schema.Type.DOUBLE), Schema.create(Schema.Type.NULL))).withDefault(0D);
            }
        }

        for(final String portfolioField : portfolioFields) {
            final String[] strs = portfolioField.split("\\.");
            final String source = strs[0];
            final String field  = strs[1];

            if(!inputSchemas.containsKey(source)) {
                throw new IllegalArgumentException();
            }

            final Schema sourceSchema = Optional
                    .ofNullable(inputSchemas.get(source))
                    .orElseThrow(() -> new IllegalArgumentException());
            final Schema.Field sourceField = Optional
                    .ofNullable(sourceSchema.getField(field))
                    .orElseThrow(() -> new IllegalArgumentException());
            schemaFields.name(field).type(sourceField.schema()).noDefault();
        }

        return schemaFields.endRecord();
    }

    private static Schema createWalletSchema() {
        return SchemaBuilder.builder().record("Wallet").fields()
                .name("market").type(Schema.create(Schema.Type.STRING)).noDefault()
                .name("currency").type(Schema.create(Schema.Type.STRING)).noDefault()
                .name("amount").type(Schema.create(Schema.Type.DOUBLE)).noDefault()
                .endRecord();
    }

}