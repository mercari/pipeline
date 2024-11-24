package com.mercari.solution.module.transform;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.mercari.solution.config.TransformConfig;
import com.mercari.solution.module.DataType;
import com.mercari.solution.module.FCollection;
import com.mercari.solution.util.pipeline.OptionUtil;

import org.apache.beam.sdk.values.*;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;


public class TradingTransformOld {

    private static final Logger LOG = LoggerFactory.getLogger(TradingTransformOld.class);

    public static class TradeTransformParameters implements Serializable {

        private List<String> groupFields;

        private JsonObject market;
        private JsonArray rules;
        private OutputType outputType;

        public List<String> getGroupFields() {
            return groupFields;
        }

        public JsonObject getMarket() {
            return market;
        }

        public JsonArray getRules() {
            return rules;
        }

        public OutputType getOutputType() {
            return this.outputType;
        }

        public void validate() {
            final List<String> errorMessages = new ArrayList<>();

            if(this.market == null || !this.market.isJsonObject()) {
                errorMessages.add("market must not be null and must be json object.");
            } else {
                if(!market.has("name") || !market.get("name").isJsonPrimitive()) {
                    errorMessages.add("market must have name property");
                }
            }

            if(this.rules == null || !this.rules.isJsonArray()) {
                errorMessages.add("rules must not be null and must be json array.");
            } else {
                int index = 0;
                for(final JsonElement jsonElement : this.rules.getAsJsonArray()) {
                    if(jsonElement == null || !jsonElement.isJsonObject()) {
                        errorMessages.add("rules[" + index + "] must be json object.");
                    } else {
                        final JsonObject jsonObject = jsonElement.getAsJsonObject();
                        if(!jsonObject.has("symbol") || !jsonObject.get("symbol").isJsonPrimitive()) {
                            errorMessages.add("rules[" + index + "] must have symbol property");
                        }
                        if(!jsonObject.has("currency") || !jsonObject.get("currency").isJsonPrimitive()) {
                            errorMessages.add("rules[" + index + "] must have currency property");
                        }
                        if(!jsonObject.has("side") || !jsonObject.get("side").isJsonPrimitive()) {
                            errorMessages.add("rules[" + index + "] must have market property");
                        }
                        if(!jsonObject.has("entry") || !jsonObject.get("entry").isJsonObject()) {
                            errorMessages.add("rules[" + index + "] must have entry json object property");
                        }
                        if(!jsonObject.has("exits") || !jsonObject.get("exits").isJsonArray()) {
                            errorMessages.add("rules[" + index + "] must have exits json array property");
                        }
                    }
                }
            }

            if(errorMessages.size() > 0) {
                throw new IllegalArgumentException(String.join("\n", errorMessages));
            }
        }

        public void setDefaults(PInput pInput) {
            if(this.groupFields == null) {
                this.groupFields = new ArrayList<>();
            }
            if(this.outputType == null) {
                if(OptionUtil.isStreaming(pInput)) {
                    this.outputType = OutputType.row;
                } else {
                    this.outputType = OutputType.avro;
                }
            }
        }

    }

    private enum OutputType {
        row,
        avro
    }



    /*
    public static Map<String, FCollection<?>> trade(final List<FCollection<?>> inputs, TransformConfig config) {

        final TradeTransformParameters parameters = new Gson().fromJson(config.getParameters(), TradeTransformParameters.class);
        if (parameters == null) {
            throw new IllegalArgumentException("TradingTransform config parameters must not be empty!");
        }
        parameters.validate();
        parameters.setDefaults(inputs.get(0).getCollection());

        final List<Schema.Field> groupFields = new ArrayList<>();
        for(final Schema.Field groupField : inputs.get(0).getSchema().getFields()) {
            if(parameters.getGroupFields().contains(groupField.getName())) {
                groupFields.add(groupField);
            }
        }

        final List<TupleTag<?>> tags = new ArrayList<>();
        final List<String> inputNames = new ArrayList<>();
        final List<DataType> inputTypes = new ArrayList<>();
        final List<SchemaUtil.StringGetter<Object>> stringGetters = new ArrayList<>();

        PCollectionTuple tuple = PCollectionTuple.empty(inputs.get(0).getCollection().getPipeline());
        for(final FCollection<?> input : inputs){
            final TupleTag tag = new TupleTag<>(){};
            tags.add(tag);
            inputNames.add(input.getName());
            inputTypes.add(input.getDataType());

            final SchemaUtil.StringGetter<Object> stringGetter = switch (input.getDataType()) {
                case ROW -> RowSchemaUtil::getAsString;
                case AVRO -> AvroSchemaUtil::getAsString;
                case STRUCT -> StructSchemaUtil::getAsString;
                case DOCUMENT -> DocumentSchemaUtil::getAsString;
                case ENTITY -> EntitySchemaUtil::getAsString;
                default -> throw new IllegalArgumentException("Not supported input type: " + input.getDataType());
            };
            stringGetters.add(stringGetter);

            tuple = tuple.and(tag, input.getCollection());
        }

        final String name = config.getName();
        final Map<String, FCollection<?>> collections = new HashMap<>();
        switch (parameters.getOutputType()) {
            case row -> {
                final Schema portfolioSchema = Portfolio.rowSchema();
                final Schema positionSchema = Position.rowSchema();
                final Trade<Schema, Schema, Row> transform = new Trade<>(
                        name,
                        parameters,
                        s -> s,
                        portfolioSchema,
                        positionSchema,
                        Portfolio::toRow,
                        Position::toRow,
                        tags,
                        inputNames,
                        inputTypes);
                final PCollectionTuple outputs = tuple.apply(name, transform);
                final PCollection<Row> outputPortfolio = outputs.get(transform.outputMainTag)
                        .setCoder(RowCoder.of(portfolioSchema));
                final PCollection<Row> outputPosition = outputs.get(transform.outputStateTag)
                        .setCoder(RowCoder.of(positionSchema));

                final FCollection<?> portfolio = FCollection.of(name, outputPortfolio, DataType.ROW, portfolioSchema);
                collections.put(name, portfolio);
                final FCollection<?> position = FCollection.of(name, outputPosition, DataType.ROW, positionSchema);
                collections.put(name + ".positions", position);
                return collections;
            }
            case avro -> {
                final org.apache.avro.Schema portfolioSchema = Portfolio.avroSchema();
                final org.apache.avro.Schema positionSchema = Position.avroSchema();
                final Trade<String, org.apache.avro.Schema, GenericRecord> transform = new Trade<>(
                        name,
                        parameters,
                        AvroSchemaUtil::convertSchema,
                        portfolioSchema.toString(),
                        positionSchema.toString(),
                        Portfolio::toRecord,
                        Position::toRecord,
                        tags,
                        inputNames,
                        inputTypes);
                final PCollectionTuple outputs = tuple.apply(name, transform);
                final PCollection<GenericRecord> outputPortfolio = outputs.get(transform.outputMainTag)
                        .setCoder(AvroCoder.of(portfolioSchema));
                final PCollection<GenericRecord> outputPosition = outputs.get(transform.outputStateTag)
                        .setCoder(AvroCoder.of(positionSchema));

                final FCollection<?> portfolio = FCollection.of(name, outputPortfolio, DataType.AVRO, portfolioSchema);
                collections.put(name, portfolio);
                final FCollection<?> position = FCollection.of(name, outputPosition, DataType.AVRO, positionSchema);
                collections.put(name + ".positions", position);
                return collections;
            }
            default -> throw new IllegalStateException("Not supported outputType: " + parameters.getOutputType());
        }
    }

    public static class Trade<InputSchemaT,RuntimeSchemaT,T> extends PTransform<PCollectionTuple, PCollectionTuple> {

        private final String name;
        private final List<String> groupFields;
        private final String marketJson;
        private final String rulesJson;

        private final SchemaUtil.SchemaConverter<InputSchemaT,RuntimeSchemaT> schemaConverter;
        private final InputSchemaT portfolioSchema;
        private final InputSchemaT positionSchema;
        private final PortfolioConverter<RuntimeSchemaT, T> portfolioConverter;
        private final PositionConverter<RuntimeSchemaT, T> positionConverter;

        // for union
        private final List<TupleTag<?>> tags;
        private final List<String> inputNames;
        private final List<DataType> inputTypes;

        // output tags
        public final TupleTag<T> outputMainTag;
        public final TupleTag<T> outputStateTag;


        Trade(final String name,
              final TradeTransformParameters parameters,
              final SchemaUtil.SchemaConverter<InputSchemaT,RuntimeSchemaT> schemaConverter,
              final InputSchemaT portfolioSchema,
              final InputSchemaT positionSchema,
              final PortfolioConverter<RuntimeSchemaT, T> portfolioConverter,
              final PositionConverter<RuntimeSchemaT, T> positionConverter,
              final List<TupleTag<?>> tags,
              final List<String> inputNames,
              final List<DataType> inputTypes) {

            this.name = name;
            this.groupFields = parameters.getGroupFields();
            this.marketJson = parameters.getMarket().toString();
            this.rulesJson = parameters.getRules().toString();

            this.schemaConverter = schemaConverter;
            this.portfolioSchema = portfolioSchema;
            this.positionSchema = positionSchema;
            this.portfolioConverter = portfolioConverter;
            this.positionConverter = positionConverter;

            this.tags = tags;
            this.inputNames = inputNames;
            this.inputTypes = inputTypes;

            this.outputMainTag = new TupleTag<>(){};
            this.outputStateTag = new TupleTag<>(){};
        }

        @Override
        public PCollectionTuple expand(PCollectionTuple input) {

            final Window window = Window.into(new GlobalWindows())
                    .triggering(Repeatedly.forever(AfterPane.elementCountAtLeast(1)))
                    .discardingFiredPanes();

            PCollectionTuple tuple = PCollectionTuple.empty(input.getPipeline());
            int count = 0;
            for(final TupleTag<?> tag : tags) {
                final PCollection collection = (PCollection) input.get(tag).apply("WithWindow" + count, window);
                tuple = tuple.and(tag, collection);
                count +=1;
            }

            final PCollection<KV<String,UnionValue>> union = tuple
                    .apply("Union", Union
                            .withKey(tags, inputTypes, groupFields, inputNames));
            final UnionCoder unionCoder = (UnionCoder)((KvCoder) union.getCoder()).getValueCoder();

            final String processName;
            final TradeDoFn<InputSchemaT,RuntimeSchemaT,T> dofn;
            if(OptionUtil.isStreaming(input)) {
                dofn = new TradeStreamingDoFn(
                        name, marketJson, rulesJson, inputNames,
                        schemaConverter, portfolioSchema, positionSchema, portfolioConverter, positionConverter,
                        outputMainTag, outputStateTag, unionCoder);
                processName = "TradeBatch";
            } else {
                dofn = new TradeBatchDoFn(
                        name, marketJson, rulesJson, inputNames,
                        schemaConverter, portfolioSchema, positionSchema, portfolioConverter, positionConverter,
                        outputMainTag, outputStateTag, unionCoder);
                processName = "TradeStreaming";
            }

            return union
                    .apply(processName, ParDo
                            .of(dofn)
                            .withOutputTags(outputMainTag, TupleTagList.of(outputStateTag)));
        }

        private static class TradeDoFn<InputSchemaT,RuntimeSchemaT,T> extends DoFn<KV<String, UnionValue>, T> {

            static final String STATE_ID_PORTFOLIO = "tradeTransformPortfolio";
            static final String STATE_ID_INPUTS = "tradeTransformInputs";

            private final TupleTag<T> outputPortfolioTag;
            private final TupleTag<T> outputPositionTag;

            private final String name;
            private final String marketJson;
            private final String rulesJson;

            private final List<String> inputNames;
            private final SchemaUtil.SchemaConverter<InputSchemaT,RuntimeSchemaT> schemaConverter;
            private final PortfolioConverter<RuntimeSchemaT, T> portfolioConverter;
            private final PositionConverter<RuntimeSchemaT, T> positionConverter;
            private final InputSchemaT portfolioInputSchema;
            private final InputSchemaT positionInputSchema;

            private transient Market market;
            private transient List<Rule> rules;
            private transient Set<String> variables;

            private transient RuntimeSchemaT portfolioRuntimeSchema;
            private transient RuntimeSchemaT positionRuntimeSchema;


            TradeDoFn(final String name,
                      final String marketJson,
                      final String rulesJson,
                      final List<String> inputNames,
                      final SchemaUtil.SchemaConverter<InputSchemaT,RuntimeSchemaT> schemaConverter,
                      final InputSchemaT portfolioSchema,
                      final InputSchemaT positionSchema,
                      final PortfolioConverter<RuntimeSchemaT, T> portfolioConverter,
                      final PositionConverter<RuntimeSchemaT, T> positionConverter,
                      final TupleTag<T> outputPortfolioTag,
                      final TupleTag<T> outputPositionTag) {

                this.name = name;
                this.marketJson = marketJson;
                this.rulesJson = rulesJson;

                this.inputNames = inputNames;

                this.schemaConverter = schemaConverter;
                this.portfolioInputSchema = portfolioSchema;
                this.positionInputSchema = positionSchema;
                this.portfolioConverter = portfolioConverter;
                this.positionConverter = positionConverter;

                this.outputPortfolioTag = outputPortfolioTag;
                this.outputPositionTag = outputPositionTag;
            }


            void setup() {
                final Gson gson = new Gson();
                final JsonObject marketObject = gson.fromJson(this.marketJson, JsonObject.class);
                this.market = Market.of(marketObject);
                this.variables = new HashSet<>();
                this.rules = new ArrayList<>();
                final JsonArray rulesJsonArray = gson.fromJson(this.rulesJson, JsonArray.class);
                for(final JsonElement jsonElement : rulesJsonArray) {
                    final Rule rule = Rule.of(jsonElement);
                    rule.setup();
                    rules.add(rule);
                    variables.addAll(rule.getRequiredVariables());
                }

                this.portfolioRuntimeSchema = schemaConverter.convert(portfolioInputSchema);
                this.positionRuntimeSchema = schemaConverter.convert(positionInputSchema);
            }

            void teardown() {

            }

            protected void processElement (
                    final KV<String, UnionValue> element,
                    final Instant timestamp,
                    final MultiOutputReceiver outputReceiver,
                    final ValueState<Portfolio> portfolioState,
                    final ValueState<Map<String, UnionValue>> inputsState) {

                final UnionValue input = element.getValue();

                final Portfolio portfolio = Optional
                        .ofNullable(portfolioState.read())
                        .orElseGet(() -> Portfolio.of(name, market.getName(), market.getWallet()));

                final Map<String, UnionValue> sourceAndInputs = Optional
                        .ofNullable(inputsState.read())
                        .orElseGet(HashMap::new);
                final String sourceName = inputNames.get(input.getIndex());
                sourceAndInputs.put(sourceName, input);
                inputsState.write(sourceAndInputs);

                // Get market information
                final MarketInfo marketInfo = market.getMarketInfo(sourceAndInputs);

                // Update existing positions state
                final List<Position> completedPositions = portfolio.update(market, sourceAndInputs, marketInfo, timestamp);
                for(final Position completedPosition : completedPositions) {
                    final T output = positionConverter.convert(positionRuntimeSchema, completedPosition);
                    outputReceiver.get(outputPositionTag).output(output);
                }

                // Create orders from rules
                final Map<String, Object> values = createInputsVariables(sourceAndInputs, variables);
                values.putAll(createWalletVariables(portfolio));
                values.putAll(createMarketVariables(marketInfo));

                final List<Order> orders = new ArrayList<>();
                for(final Rule rule : rules) {
                    final List<Order> newOrders = rule.apply(portfolio, values, timestamp);
                    orders.addAll(newOrders);
                }

                // Execute orders
                portfolio.order(market, orders, timestamp);

                // Update portfolio state
                portfolioState.write(portfolio);

                //System.out.println("last portfolio: " + portfolio);

                // Generate output and send
                final T output = portfolioConverter.convert(portfolioRuntimeSchema, portfolio);
                outputReceiver.get(outputPortfolioTag).output(output);

            }

            protected void flush(final MultiOutputReceiver outputReceiver,
                                 final ValueState<Portfolio> portfolioState,
                                 final ValueState<Map<String, UnionValue>> inputsState) {

                final Portfolio portfolio = portfolioState.read();
                if(portfolio == null) {
                    return;
                }

                for(final Position completedPosition : portfolio.getPositions()) {
                    final T output = positionConverter.convert(positionRuntimeSchema, completedPosition);
                    outputReceiver.get(outputPositionTag).output(output);
                }
            }

            private static Map<String, Object> createInputsVariables(
                    final Map<String, UnionValue> sourceAndInputs,
                    final Set<String> variables) {

                final Map<String, Object> values = new HashMap<>();
                for(final Map.Entry<String, UnionValue> sourceAndInput : sourceAndInputs.entrySet()) {
                    final Set<String> fieldsNames = variables.stream()
                            .map(v -> {
                                if(v.contains(".")) {
                                    final String[] strs = v.split("\\.");
                                    if(strs[0].equals(sourceAndInput.getKey())) {
                                        return strs[strs.length - 1];
                                    }
                                    return null;
                                } else {
                                    return v;
                                }
                            })
                            .filter(Objects::nonNull)
                            .collect(Collectors.toSet());
                    final Map<String, Object> value = sourceAndInput.getValue().getMap(fieldsNames);
                    for(final Map.Entry<String, Object> keyValue : value.entrySet()) {
                        final String key = String.format("%s.%s", sourceAndInput.getKey(), keyValue.getKey());
                        values.put(key, keyValue.getValue());
                    }
                }
                return values;
            }

            private static Map<String, Double> createWalletVariables(final Portfolio portfolio) {
                final Map<String, Double> values = new HashMap<>();
                for(final Map.Entry<String, Double> currency : portfolio.getWallet().entrySet()) {
                    final String key = String.format("__wallet.%s", currency.getKey());
                    values.put(key, currency.getValue());
                }
                return values;
            }

            private static Map<String, Double> createMarketVariables(final MarketInfo marketInfo) {
                final Map<String, Double> values = new HashMap<>();
                return values;
            }

        }

        private class TradeBatchDoFn extends TradeDoFn<InputSchemaT,RuntimeSchemaT,T> {

            @StateId(STATE_ID_PORTFOLIO)
            private final StateSpec<ValueState<Portfolio>> portfolioSpec;
            @StateId(STATE_ID_INPUTS)
            private final StateSpec<ValueState<Map<String, UnionValue>>> inputsSpec;

            TradeBatchDoFn(final String name,
                           final String marketsJson,
                           final String rulesJson,
                           final List<String> inputNames,
                           final SchemaUtil.SchemaConverter<InputSchemaT,RuntimeSchemaT> schemaConverter,
                           final InputSchemaT portfolioSchema,
                           final InputSchemaT positionSchema,
                           final PortfolioConverter<RuntimeSchemaT, T> portfolioConverter,
                           final PositionConverter<RuntimeSchemaT, T> positionConverter,
                           final TupleTag<T> outputPortfolioTag,
                           final TupleTag<T> outputPositionTag,
                           final UnionCoder inputsCoder) {

                super(name, marketsJson, rulesJson, inputNames,
                        schemaConverter, portfolioSchema, positionSchema, portfolioConverter, positionConverter,
                        outputPortfolioTag, outputPositionTag);

                this.portfolioSpec = StateSpecs.value(AvroCoder.of(Portfolio.class));
                this.inputsSpec = StateSpecs.value(MapCoder.of(StringUtf8Coder.of(), inputsCoder));
            }

            @Setup
            public void setup() {
                super.setup();
            }

            @ProcessElement
            @RequiresTimeSortedInput
            public void processElement(
                    final @Element KV<String, UnionValue> element,
                    final @Timestamp Instant timestamp,
                    final DoFn.MultiOutputReceiver outputReceiver,
                    final @AlwaysFetched @StateId(STATE_ID_PORTFOLIO) ValueState<Portfolio> portfolioSpec,
                    final @AlwaysFetched @StateId(STATE_ID_INPUTS) ValueState<Map<String,UnionValue>> inputsState) {

                super.processElement(element, timestamp, outputReceiver, portfolioSpec, inputsState);
            }

            @OnWindowExpiration
            public void onWindowExpiration(
                    final DoFn.MultiOutputReceiver outputReceiver,
                    final @AlwaysFetched @StateId(STATE_ID_PORTFOLIO) ValueState<Portfolio> portfolioState,
                    final @AlwaysFetched @StateId(STATE_ID_INPUTS) ValueState<Map<String,UnionValue>> inputsState) {

                LOG.info("onWindowExpiration");

                flush(outputReceiver, portfolioState, inputsState);
            }

            @Teardown
            public void teardown() {
                super.teardown();
            }
        }

        private class TradeStreamingDoFn extends TradeDoFn<InputSchemaT,RuntimeSchemaT,T> {

            @StateId(STATE_ID_PORTFOLIO)
            private final StateSpec<ValueState<Portfolio>> portfolioSpec;
            @StateId(STATE_ID_INPUTS)
            private final StateSpec<ValueState<Map<String, UnionValue>>> inputsSpec;

            TradeStreamingDoFn(final String name,
                               final String marketsJson,
                               final String rulesJson,
                               final List<String> inputNames,
                               final SchemaUtil.SchemaConverter<InputSchemaT,RuntimeSchemaT> schemaConverter,
                               final InputSchemaT portfolioSchema,
                               final InputSchemaT positionSchema,
                               final PortfolioConverter<RuntimeSchemaT, T> portfolioConverter,
                               final PositionConverter<RuntimeSchemaT, T> positionConverter,
                               final TupleTag<T> outputPortfolioTag,
                               final TupleTag<T> outputPositionTag,
                               final UnionCoder inputsCoder) {

                super(name, marketsJson, rulesJson, inputNames,
                        schemaConverter, portfolioSchema, positionSchema, portfolioConverter, positionConverter,
                        outputPortfolioTag, outputPositionTag);

                this.portfolioSpec = StateSpecs.value(AvroCoder.of(Portfolio.class));
                this.inputsSpec = StateSpecs.value(MapCoder.of(StringUtf8Coder.of(), inputsCoder));
            }

            @Setup
            public void setup() {
                super.setup();
            }

            @ProcessElement
            public void processElement(
                    final @Element KV<String, UnionValue> element,
                    final @Timestamp Instant timestamp,
                    final DoFn.MultiOutputReceiver outputReceiver,
                    final @AlwaysFetched @StateId(STATE_ID_PORTFOLIO) ValueState<Portfolio> portfolioState,
                    final @AlwaysFetched @StateId(STATE_ID_INPUTS) ValueState<Map<String,UnionValue>> inputsState) {

                super.processElement(element, timestamp, outputReceiver, portfolioState, inputsState);
            }

            @OnWindowExpiration
            public void onWindowExpiration(
                    final DoFn.MultiOutputReceiver outputReceiver,
                    final @AlwaysFetched @StateId(STATE_ID_PORTFOLIO) ValueState<Portfolio> portfolioState,
                    final @AlwaysFetched @StateId(STATE_ID_INPUTS) ValueState<Map<String,UnionValue>> inputsState) {

                LOG.info("onWindowExpiration");

                flush(outputReceiver, portfolioState, inputsState);
            }

            @Teardown
            public void teardown() {
                super.teardown();
            }
        }

    }

    private interface OrderBooksConverter<T> extends Serializable {
        OrderBooks convert(T orderBooks);
    }

    private interface TradeBooksConverter<T> extends Serializable {
        TradeBooks convert(T tradeBooks);
    }

    private interface PortfolioConverter<RuntimeSchemaT,T> extends Serializable {
        T convert(RuntimeSchemaT schema, Portfolio portfolio);
    }

    private interface PositionConverter<RuntimeSchemaT,T> extends Serializable {
        T convert(RuntimeSchemaT schema, Position position);
    }

     */

}