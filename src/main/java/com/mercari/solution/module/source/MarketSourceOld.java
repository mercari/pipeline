package com.mercari.solution.module.source;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.mercari.solution.config.SourceConfig;
import com.mercari.solution.module.DataType;
import com.mercari.solution.module.FCollection;
import com.mercari.solution.util.pipeline.OptionUtil;
import com.mercari.solution.util.TemplateUtil;
import com.mercari.solution.util.domain.finance.trading.event.*;
import com.mercari.solution.util.domain.finance.trading.market.Market;
import com.mercari.solution.util.schema.AvroSchemaUtil;
import com.mercari.solution.util.schema.SchemaUtil;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.coders.*;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.*;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.WebSocketHandshakeException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;


public class MarketSourceOld {

    private static final Logger LOG = LoggerFactory.getLogger(MarketSourceOld.class);

    private static class MarketSourceParameters implements Serializable {

        private String market;
        private EventType type;
        private AssetType asset;
        private List<String> pairs;

        private Long intervalSeconds;
        private Long heartbeatIntervalSeconds;
        private Long requestIntervalSeconds;

        private Format outputType;


        public String getMarket() {
            return market;
        }

        public EventType getType() {
            return type;
        }

        public AssetType getAsset() {
            return asset;
        }

        public List<String> getPairs() {
            return pairs;
        }

        public Long getIntervalSeconds() {
            return intervalSeconds;
        }

        public Long getHeartbeatIntervalSeconds() {
            return heartbeatIntervalSeconds;
        }

        public Long getRequestIntervalSeconds() {
            return requestIntervalSeconds;
        }

        public Format getOutputType() {
            return outputType;
        }


        private void validate(final PBegin begin) {

            if(!OptionUtil.isStreaming(begin.getPipeline().getOptions())) {
                throw new IllegalArgumentException("Market source module only support streaming mode.");
            }

            // check required parameters filled
            final List<String> errorMessages = new ArrayList<>();
            if(this.market == null) {
                errorMessages.add("Market source module requires endpoint parameter");
            }
            if(this.type == null) {
                errorMessages.add("Market source module requires type parameter");
            }
            // check optional parameters
            if(this.getIntervalSeconds() != null) {
                if(this.getIntervalSeconds() < 1) {
                    errorMessages.add("WebSocket source module intervalSeconds parameter must over zero");
                }
            }
            if(this.getHeartbeatIntervalSeconds() != null) {
                if(this.getHeartbeatIntervalSeconds() < 0) {
                    errorMessages.add("WebSocket source module heartbeatIntervalSeconds parameter must over zero");
                }
            }
            if(errorMessages.size() > 0) {
                throw new IllegalArgumentException(String.join(", ", errorMessages));
            }
        }

        private void setDefaults() {
            if(this.getIntervalSeconds() == null) {
                this.intervalSeconds = 1L;
            }
            if(this.getHeartbeatIntervalSeconds() == null) {
                this.heartbeatIntervalSeconds = 60L;
            }
            if(this.getRequestIntervalSeconds() == null) {
                this.requestIntervalSeconds = 0L;
            }
            if(this.outputType == null) {
                switch (type) {
                    case ticker, trade -> {
                        this.outputType = Format.avro;
                    }
                    case orderbooks -> {
                        this.outputType = Format.avro;
                    }
                    default -> throw new IllegalArgumentException();
                }
            }
        }
    }

    private enum Format {
        row,
        avro
    }

    public String getName() { return "market"; }


    public Map<String, FCollection<?>> expand(PBegin begin, SourceConfig config, PCollection<Long> beats, List<FCollection<?>> waits) {
        if(OptionUtil.isStreaming(begin)) {
            return stream(begin, config);
        } else {
            throw new IllegalArgumentException("Market source module support only streaming mode");
        }
    }

    public static Map<String, FCollection<?>> stream(final PBegin begin, final SourceConfig config) {

        final MarketSourceParameters parameters = new Gson().fromJson(config.getParameters(), MarketSourceParameters.class);
        if(parameters == null) {
            throw new IllegalArgumentException("Market source module parameters must not be empty!");
        }

        parameters.validate(begin);
        parameters.setDefaults();

        final JsonObject params = new JsonObject();
        params.addProperty("name", parameters.getMarket());

        final Market market = Market.of(params);

        final String failuresOutputName = config.getName() + ".failures";

        final Map<String, FCollection<?>> results = new HashMap<>();
        switch (parameters.getOutputType()) {
            case row -> {
                final Schema outputSchema = switch (parameters.getType()) {
                    case ticker -> Ticker.schema;
                    case trade -> Trade.schema;
                    case orderbooks -> OrderBooks.schema;
                };

                final MarketSourceStream<Schema,Schema,Row> stream = new MarketSourceStream<>(
                        config.getName(), market, parameters, Trade::create, OrderBooks::create, s -> s, outputSchema);
                final PCollectionTuple outputs = begin.apply(config.getName(), stream);

                results.put(config.getName(), FCollection
                        .of(config.getName(), outputs.get(stream.outputTag).setCoder(RowCoder.of(outputSchema)), DataType.ROW, outputSchema));

                results.put(failuresOutputName, FCollection
                        .of(failuresOutputName, outputs.get(stream.failuresTag).setCoder(RowCoder.of(FailureMessage.schema)), DataType.ROW, FailureMessage.schema));
            }
            case avro -> {
                final org.apache.avro.Schema outputSchema = switch (parameters.getType()) {
                    case ticker -> Ticker.createAvroSchema();
                    case trade -> Trade.createAvroSchema();
                    case orderbooks -> OrderBooks.createAvroSchema();
                };

                final MarketSourceStream<String,org.apache.avro.Schema,GenericRecord> stream = new MarketSourceStream<>(
                        config.getName(), market, parameters, Trade::create, OrderBooks::create, AvroSchemaUtil::convertSchema, outputSchema.toString());
                final PCollectionTuple outputs = begin.apply(config.getName(), stream);

                results.put(config.getName(), FCollection
                        .of(config.getName(), outputs.get(stream.outputTag).setCoder(AvroCoder.of(outputSchema)), DataType.AVRO, outputSchema));

                final org.apache.avro.Schema failuresSchema = FailureMessage.createAvroSchema();
                results.put(failuresOutputName, FCollection
                        .of(failuresOutputName, outputs.get(stream.failuresTag).setCoder(AvroCoder.of(failuresSchema)), DataType.AVRO, failuresSchema));

            }
            default -> throw new IllegalArgumentException("Market source not supported output type: " + parameters.getOutputType());
        }

        return results;
    }

    private static class MarketSourceStream<InputSchemaT,RuntimeSchemaT,T> extends PTransform<PBegin, PCollectionTuple> {

        private final TupleTag<T> outputTag;
        private final TupleTag<T> failuresTag;

        private final String name;
        private final Market market;
        private final EventType eventType;
        private final AssetType assetType;
        private final List<String> pairs;
        private final Long intervalMillis;
        private final Long heartbeatIntervalMillis;
        private final Long requestIntervalMillis;

        private final Market.TradeCreator<RuntimeSchemaT,T> tradeCreator;
        private final Market.OrderBooksCreator<RuntimeSchemaT,T> orderBooksCreator;
        private final SchemaUtil.SchemaConverter<InputSchemaT,RuntimeSchemaT> schemaConverter;
        private final InputSchemaT outputSchema;


        //private final DataType dataType;

        private MarketSourceStream(
                final String name,
                final Market market,
                final MarketSourceParameters parameters,
                final Market.TradeCreator<RuntimeSchemaT,T> tradeCreator,
                final Market.OrderBooksCreator<RuntimeSchemaT,T> orderBooksCreator,
                final SchemaUtil.SchemaConverter<InputSchemaT,RuntimeSchemaT> schemaConverter,
                final InputSchemaT outputSchema) {

            this.outputTag = new TupleTag<>() {};
            this.failuresTag = new TupleTag<>() {};

            this.name = name;
            this.market = market;
            this.eventType = parameters.getType();
            this.assetType = parameters.getAsset();
            this.pairs = parameters.getPairs();
            this.intervalMillis = parameters.getIntervalSeconds() * 1000L;
            this.heartbeatIntervalMillis = parameters.getHeartbeatIntervalSeconds() * 1000L;
            this.requestIntervalMillis = parameters.getRequestIntervalSeconds() * 1000L;

            this.tradeCreator = tradeCreator;
            this.orderBooksCreator = orderBooksCreator;
            this.schemaConverter = schemaConverter;
            this.outputSchema = outputSchema;
        }

        public PCollectionTuple expand(final PBegin begin) {
            final PCollection<KV<String, List<KV<Instant, String>>>> symbolAndMessages = begin
                    .apply("GenerateSequence", GenerateSequence
                            .from(0)
                            .withRate(1, Duration.millis(intervalMillis)))
                    .apply("WithKey", MapElements
                            .into(TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.longs()))
                            .via((beat) -> KV.of("", beat)))
                    .apply("ReceiveMessages", ParDo.of(new WebSocketDoFn(
                            name, market, eventType, assetType, pairs,
                            heartbeatIntervalMillis, requestIntervalMillis, requestIntervalMillis)));
                    //.apply("ReceiveMessages", ParDo.of(new SocketDoFn(
                    //        name, market, eventType, assetType, pairs)));

            return switch (eventType) {
                case trade -> symbolAndMessages
                        .apply("ToTrade", ParDo.of(new ConvertTradeDoFn<>(
                                name, market, assetType, tradeCreator, schemaConverter, outputSchema, outputTag, failuresTag))
                                .withOutputTags(outputTag, TupleTagList.of(failuresTag)));
                case orderbooks -> symbolAndMessages
                        .apply("ToOrderBooks", ParDo.of(new ConvertOrderBooksDoFn<>(
                                name, market, assetType, orderBooksCreator, schemaConverter, outputSchema, outputTag, failuresTag))
                                .withOutputTags(outputTag, TupleTagList.of(failuresTag)));
                default -> throw new IllegalArgumentException();
            };
        }

        /*
        private static class SocketDoFn extends DoFn<KV<String, Long>, KV<String, List<KV<Instant, String>>>> {

            private static final String STATE_ID_CHECK_EPOCH_MILLIS = "checkPrevState";
            private static final String STATE_ID_HEARTBEAT_EPOCH_MILLIS = "heartbeatPrevState";
            private static final String STATE_ID_INTERVAL_MESSAGE_COUNT = "intervalMessageCountState";

            @StateId(STATE_ID_CHECK_EPOCH_MILLIS)
            private final StateSpec<ValueState<Long>> prevCheckStateSpec = StateSpecs.value(BigEndianLongCoder.of());
            @StateId(STATE_ID_HEARTBEAT_EPOCH_MILLIS)
            private final StateSpec<ValueState<Long>> prevHeartbeatStateSpec = StateSpecs.value(BigEndianLongCoder.of());
            @StateId(STATE_ID_INTERVAL_MESSAGE_COUNT)
            private final StateSpec<ValueState<Integer>> intervalMessageCountStateSpec = StateSpecs.value(BigEndianIntegerCoder.of());

            private final String name;
            private final Market market;
            private final EventType eventType;
            private final AssetType assetType;
            private final List<String> pairs;

            private final List<String> heartbeatRequests;
            private final Long heartbeatIntervalMillis;
            private final Long checkIntervalMillis;

            private transient String endpoint;
            private transient okhttp3.WebSocket socket;
            private transient Listener listener;
            private transient List<String> symbols;


            SocketDoFn(final String name,
                       final Market market,
                       final EventType eventType,
                       final AssetType assetType,
                       final List<String> pairs) {

                this.name = name;
                this.market = market;
                this.eventType = eventType;
                this.assetType = assetType;
                this.pairs = pairs;

                this.heartbeatRequests = market.createHeartbeatRequests();
                this.heartbeatIntervalMillis = market.getHeartbeatIntervalMillis(eventType, assetType);
                this.checkIntervalMillis = market.getCheckIntervalMillis(eventType, assetType, pairs);
            }

            private void connect() {
                final List<String> requests = market.createWebSocketRequests(eventType, assetType, symbols);
                this.listener = new Listener(name, requests);
                listener.status = Listener.ListenerStatus.opening;

                final okhttp3.Request request = new okhttp3.Request.Builder()
                        .url(endpoint)
                        .get()
                        .build();
                this.socket = new okhttp3.OkHttpClient()
                        .newBuilder()
                        .pingInterval(java.time.Duration.ofSeconds(20L))
                        .build()
                        .newWebSocket(request, this.listener);
                LOG.info("SocketDoFn[" + name + "] connected");
            }

            @Setup
            public void setup() {
                LOG.info("SocketDoFn[" + name + "] setup");
                this.symbols = market.convertSymbols(eventType, assetType, pairs);
                this.endpoint = market.createWebSocketEndpoint(eventType, assetType, symbols);
                LOG.info("SocketDoFn[" + name + "] setup endpoint: " + endpoint + ", symbols: " + symbols);
                connect();
            }

            @Teardown
            public void teardown() {
                LOG.info("SocketDoFn[" + name + "] teardown");
                if(this.socket != null) {
                    final boolean closed = socket.close(0, "close");
                    LOG.info("SocketDoFn[" + name + "] teardown. closed socket: " + closed);
                }
            }

            @ProcessElement
            public void processElement(final ProcessContext c,
                                       final @StateId(STATE_ID_CHECK_EPOCH_MILLIS) @AlwaysFetched ValueState<Long> prevCheckEpochMillisState,
                                       final @StateId(STATE_ID_HEARTBEAT_EPOCH_MILLIS) @AlwaysFetched ValueState<Long> prevHeartbeatEpochMillisState,
                                       final @StateId(STATE_ID_INTERVAL_MESSAGE_COUNT) @AlwaysFetched ValueState<Integer> intervalMessageCountState) {

                int messageCount = Optional.ofNullable(intervalMessageCountState.read()).orElse(0);
                synchronized (listener) {
                    if(Listener.ListenerStatus.closed.equals(listener.getStatus())) {
                        listener.status = Listener.ListenerStatus.opening;
                        connect();
                    }
                    final List<KV<Instant, String>> messages = listener.getMessages();
                    messageCount += messages.size();

                    final Map<String, List<KV<Instant, String>>> outputs = new HashMap<>();
                    for(final KV<Instant, String> message : messages) {
                        final String symbol = market.extractSymbol(eventType, assetType, message.getValue());
                        if(symbol == null) {
                            //LOG.warn("Failed to get symbol from message: " + message.getValue());
                            continue;
                        }
                        if(!outputs.containsKey(symbol)) {
                            outputs.put(symbol, new ArrayList<>());
                        }
                        outputs.get(symbol).add(message);
                    }

                    for(Map.Entry<String, List<KV<Instant, String>>> entry : outputs.entrySet()) {
                        c.output(KV.of(entry.getKey(), entry.getValue()));
                    }
                    listener.clearMessages();
                }

                if(this.checkIntervalMillis > 0L) {
                    final long nextEpochMillis = Instant.now().getMillis();
                    final long prevEpochMillis = Optional.ofNullable(prevCheckEpochMillisState.read()).orElse(nextEpochMillis);
                    if(nextEpochMillis - prevEpochMillis > this.checkIntervalMillis) {
                        if(messageCount == 0) {
                            LOG.warn("SocketDoFn[" + name + "] try to re-connect because no messages in fixed milli secs: " + this.checkIntervalMillis);
                            connect();
                        }
                        messageCount = 0;
                        prevCheckEpochMillisState.write(nextEpochMillis);
                        final List<String> newSymbols = market.convertSymbols(eventType, assetType, pairs);
                        if(this.symbols != null && newSymbols != null && (!this.symbols.containsAll(newSymbols) || !newSymbols.containsAll(this.symbols))) {
                            this.symbols = newSymbols;
                        }
                    } else if(nextEpochMillis == prevEpochMillis) {
                        // First time initializing state
                        prevCheckEpochMillisState.write(nextEpochMillis);
                    }
                }
                intervalMessageCountState.write(messageCount);

                if(this.heartbeatRequests != null && this.heartbeatRequests.size() > 0 && this.heartbeatIntervalMillis > 0L) {
                    final long nextEpochMillis = Instant.now().getMillis();
                    final long prevEpochMillis = Optional.ofNullable(prevHeartbeatEpochMillisState.read()).orElse(nextEpochMillis);
                    if(nextEpochMillis - prevEpochMillis > this.heartbeatIntervalMillis) {
                        for(final String heartbeatRequest : heartbeatRequests) {
                            this.socket.send(heartbeatRequest);
                        }
                        prevHeartbeatEpochMillisState.write(nextEpochMillis);
                    } else if(nextEpochMillis == prevEpochMillis) {
                        // First time initializing state
                        prevHeartbeatEpochMillisState.write(nextEpochMillis);
                    }
                }

            }

            private static class Listener extends okhttp3.WebSocketListener {

                private final String name;
                private ListenerStatus status;
                private final List<String> requests;

                private final List<KV<Instant, String>> messages;

                Listener(final String name, final List<String> requests) {
                    this.name = name;
                    this.status = ListenerStatus.closed;
                    this.requests = requests;
                    this.messages = Collections.synchronizedList(new ArrayList<>());
                }

                enum ListenerStatus {
                    opening,
                    open,
                    closed;
                }

                @Override
                public void onOpen(okhttp3.WebSocket webSocket, okhttp3.Response response) {
                    LOG.info("Socket[" + name + "].listener.onOpen");
                    super.onOpen(webSocket, response);
                    this.status = ListenerStatus.open;
                    if(requests != null) {
                        final Map<String, Object> data = new HashMap<>();
                        for(final String request : requests) {
                            final String body = TemplateUtil.executeStrictTemplate(request, data);
                            webSocket.send(body);
                            try {
                                Thread.sleep(1000L);
                            } catch (final InterruptedException e) {
                                LOG.warn("Socket[" + name + "].onOpen throws exception: " + e);
                            }
                        }
                    }
                }

                @Override
                public void onFailure(okhttp3.WebSocket webSocket, Throwable t, okhttp3.Response response) {
                    final String message = "Socket[" + name + "].listener.onFailure. cause: " + t.getCause() + ", message: " + t.getMessage() + ". response: " + (response == null ? "" : response.message());
                    LOG.error(message);
                    super.onFailure(webSocket, t, response);
                    this.status = ListenerStatus.closed;
                    throw new IllegalStateException(message);
                }

                @Override
                public void onMessage(okhttp3.WebSocket webSocket, String text) {
                    super.onMessage(webSocket, text);
                    if("pong".equalsIgnoreCase(text)) {
                        return;
                    } else if("ping".equalsIgnoreCase(text)) {
                        webSocket.send("pong");
                        return;
                    }
                    this.messages.add(KV.of(Instant.now(), text));
                }

                @Override
                public void onClosed(okhttp3.WebSocket webSocket, int code, String reason) {
                    LOG.info("Socket[" + name + "].listener.onClosed. code: " + code + ", reason: " + reason);
                    super.onClosed(webSocket, code, reason);
                    this.status = ListenerStatus.closed;
                }

                @Override
                public void onClosing(okhttp3.WebSocket webSocket, int code, String reason) {
                    LOG.info("Socket[" + name + "].listener.onClosing. code: " + code + ", reason: " + reason);
                    super.onClosing(webSocket, code, reason);
                    this.status = ListenerStatus.closed;
                }

                public ListenerStatus getStatus() {
                    return this.status;
                }

                public List<KV<Instant, String>> getMessages() {
                    return new ArrayList<>(this.messages);
                }

                public void clearMessages() {
                    this.messages.clear();
                }

                public void reconnect(okhttp3.WebSocket webSocket) {
                    if(requests != null) {
                        final Map<String, Object> data = new HashMap<>();
                        for(final String request : requests) {
                            final String body = TemplateUtil.executeStrictTemplate(request, data);
                            webSocket.send(body);
                            try {
                                Thread.sleep(1000L);
                            } catch (final InterruptedException e) {
                                LOG.warn("Socket[" + name + "].onOpen throws exception: " + e);
                            }
                        }
                    }
                }
            }

         */

        }

        private static class WebSocketDoFn extends DoFn<KV<String, Long>, KV<String, List<KV<Instant, String>>>> {

            private static final String STATE_ID_BEAT = "beatState";
            private static final String STATE_ID_HEARTBEAT_EPOCH_MILLIS = "heartbeatPrevState";
            private static final String STATE_ID_CHECK_EPOCH_MILLIS = "checkPrevState";
            private static final String STATE_ID_INTERVAL_MESSAGE_COUNT = "intervalMessageCountState";

            @StateId(STATE_ID_BEAT)
            private final StateSpec<ValueState<Long>> beatStateSpec = StateSpecs.value(BigEndianLongCoder.of());
            @StateId(STATE_ID_HEARTBEAT_EPOCH_MILLIS)
            private final StateSpec<ValueState<Long>> prevHeartbeatStateSpec = StateSpecs.value(BigEndianLongCoder.of());
            @StateId(STATE_ID_CHECK_EPOCH_MILLIS)
            private final StateSpec<ValueState<Long>> prevCheckStateSpec = StateSpecs.value(BigEndianLongCoder.of());
            @StateId(STATE_ID_INTERVAL_MESSAGE_COUNT)
            private final StateSpec<ValueState<Integer>> intervalMessageCountStateSpec = StateSpecs.value(BigEndianIntegerCoder.of());

            private final String name;
            private final Market market;
            private final String endpoint;
            private final List<String> requests;
            private final List<String> heartbeatRequests;
            private final Long heartbeatIntervalMillis;
            private final Long checkIntervalMillis;
            private final Long requestIntervalMillis;

            private transient java.net.http.WebSocket socket;
            private transient Listener listener;

            WebSocketDoFn(final String name,
                          final Market market,
                          final EventType eventType,
                          final AssetType assetType,
                          final List<String> symbols,
                          final Long heartbeatIntervalMillis,
                          final Long checkIntervalMillis,
                          final Long requestIntervalMillis) {

                this.name = name;
                this.market = market;

                this.endpoint = market.createWebSocketEndpoint(eventType, assetType, symbols);
                this.requests = market.createWebSocketRequests(eventType, assetType, symbols);
                this.heartbeatRequests = market.createWebSocketRequests(eventType, assetType, symbols);
                this.heartbeatIntervalMillis = heartbeatIntervalMillis;
                this.checkIntervalMillis = checkIntervalMillis;
                this.requestIntervalMillis = requestIntervalMillis;
            }

            private void connect() throws InterruptedException, ExecutionException {
                this.listener = new Listener(name, requests);
                this.socket = HttpClient
                        .newHttpClient()
                        .newWebSocketBuilder()
                        .buildAsync(URI.create(endpoint), listener)
                        .get();
                LOG.info("WebSocket[" + name + "] connected");
            }

            @Setup
            public void setup() throws InterruptedException {
                LOG.info("WebSocket[" + name + "] setup");
                try {
                    connect();
                } catch (final ExecutionException e) {
                    final String message;
                    if(e.getCause() instanceof WebSocketHandshakeException webSocketHandshakeException) {
                        message = "WebSocket[" + name + "] setup failed to connect. WebSocketHandshakeException.statusCode: "
                                + webSocketHandshakeException.getResponse().statusCode()
                                + ", body: "  + webSocketHandshakeException.getResponse().body();
                        if(webSocketHandshakeException.getResponse().statusCode() >= 500) {
                            LOG.error(message);
                            return;
                        }
                    } else {
                        message = "WebSocket[" + name + "] setup failed to connect. cause: " + e.getCause() + ", message: "  + e.getMessage();
                    }
                    LOG.error(message);
                    throw new IllegalStateException(message, e);
                }
            }

            @Teardown
            public void teardown() throws InterruptedException, ExecutionException {
                LOG.info("WebSocket[" + name + "] teardown");
                final CompletableFuture<java.net.http.WebSocket> comp = socket.sendClose(java.net.http.WebSocket.NORMAL_CLOSURE, "");
                this.socket = comp.get();
            }

            @ProcessElement
            public void processElement(final ProcessContext c,
                                       final @StateId(STATE_ID_BEAT) ValueState<Long> beatState,
                                       final @StateId(STATE_ID_HEARTBEAT_EPOCH_MILLIS) ValueState<Long> prevHeartbeatEpochMillisState,
                                       final @StateId(STATE_ID_CHECK_EPOCH_MILLIS) ValueState<Long> prevCheckEpochMillisState,
                                       final @StateId(STATE_ID_INTERVAL_MESSAGE_COUNT) @AlwaysFetched ValueState<Integer> intervalMessageCountState)
                    throws InterruptedException, ExecutionException {

                int messageCount = Optional.ofNullable(intervalMessageCountState.read()).orElse(0);
                synchronized (listener) {
                    final List<KV<Instant, String>> messages = listener.getMessages();
                    messageCount += messages.size();
                    c.output(KV.of(c.element().getKey(), new ArrayList<>(messages)));
                    listener.clearMessages();
                }

                if(this.listener.isClosed()) {
                    LOG.warn("WebSocket[" + name + "].listener is closed. Start connection");
                    connect();
                } else if(this.socket.isOutputClosed()) {
                    LOG.warn("WebSocket[" + name + "].socket output is closed. Start connection");
                    connect();
                } else if(this.socket.isInputClosed()) {
                    LOG.warn("WebSocket[" + name + "].socket input is closed. Start connection");
                    connect();
                } else {
                    if(this.heartbeatRequests != null && this.heartbeatRequests.size() > 0) {
                        final long nextEpochMillis = Instant.now().getMillis();
                        final long prevEpochMillis = Optional.ofNullable(prevHeartbeatEpochMillisState.read()).orElse(0L);
                        if(nextEpochMillis - prevEpochMillis > this.heartbeatIntervalMillis) {
                            for (final String request : this.heartbeatRequests) {
                                this.socket.sendText(request, true);
                            }
                            prevHeartbeatEpochMillisState.write(nextEpochMillis);
                        }
                    }
                    if(this.checkIntervalMillis > 0L) {
                        final long nextEpochMillis = Instant.now().getMillis();
                        final long prevEpochMillis = Optional.ofNullable(prevCheckEpochMillisState.read()).orElse(nextEpochMillis);
                        if(nextEpochMillis - prevEpochMillis > this.checkIntervalMillis) {
                            if (messageCount == 0) {
                                LOG.warn("WebSocket[" + name + "] no message in check interval millis: " + checkIntervalMillis + ". Start to re-connection");
                                if(socket != null) {
                                    LOG.warn("WebSocket[" + name + "] isInputClosed: " + socket.isInputClosed() + ", isOutputClosed: " + socket.isOutputClosed());
                                }
                                try {
                                    connect();
                                } catch (final Throwable e) {
                                    LOG.error("WebSocket[" + name + "] failed to re-connection cause: " + e.getMessage());
                                }
                            }
                            messageCount = 0;
                            prevCheckEpochMillisState.write(nextEpochMillis);
                        } else if(nextEpochMillis == prevEpochMillis) {
                            // First time initializing state
                            prevCheckEpochMillisState.write(nextEpochMillis);
                        }
                    }
                }

                beatState.write(c.element().getValue());
                intervalMessageCountState.write(messageCount);
            }

            private class Listener implements java.net.http.WebSocket.Listener {

                private final String name;
                private StringBuilder stringBuffer;
                private ByteBuffer byteBuffer;
                private boolean isClosed = true;
                private final List<String> requests;

                private final List<KV<Instant, String>> messages;

                Listener(final String name, final List<String> requests) {
                    this.name = name;
                    this.requests = requests;
                    this.messages = Collections.synchronizedList(new ArrayList<>());
                }

                @Override
                public void onOpen(java.net.http.WebSocket webSocket) {
                    this.stringBuffer = new StringBuilder();
                    this.byteBuffer = ByteBuffer.allocate(100000);
                    java.net.http.WebSocket.Listener.super.onOpen(webSocket);
                    LOG.info("WebSocket[" + name + "] onOpen");
                    this.isClosed = false;
                    if(requests != null) {
                        final Map<String, Object> data = new HashMap<>();
                        for(final String request : requests) {
                            final String body = TemplateUtil.executeStrictTemplate(request, data);
                            webSocket.sendText(body, true);
                            LOG.info("WebSocket[" + name + "] send message: " + body);
                            if(requestIntervalMillis > 0) {
                                try {
                                    Thread.sleep(requestIntervalMillis);
                                } catch (InterruptedException e) {
                                    LOG.warn("WebSocket[" + name + "] throws exception: " + e);
                                }
                            }
                        }
                    }
                }

                @Override
                public CompletionStage<?> onText(java.net.http.WebSocket webSocket, CharSequence data, boolean last) {
                    this.stringBuffer.append(data);
                    if(last) {
                        final String message = this.stringBuffer.toString();
                        this.stringBuffer.setLength(0);
                        this.messages.add(KV.of(Instant.now(), message));
                    }
                    return java.net.http.WebSocket.Listener.super.onText(webSocket, data, last);
                }

                @Override
                public CompletionStage<?> onBinary(java.net.http.WebSocket webSocket, ByteBuffer data, boolean last) {
                    this.byteBuffer.put(data);
                    if(last) {
                        final ByteBuffer message = this.byteBuffer.asReadOnlyBuffer();
                        this.byteBuffer.clear();
                        this.messages.add(KV.of(Instant.now(), message.toString()));
                    }
                    return java.net.http.WebSocket.Listener.super.onBinary(webSocket, data, last);
                }

                @Override
                public CompletionStage<?> onPing(java.net.http.WebSocket webSocket, ByteBuffer message) {
                    LOG.debug("WebSocket[" + name + "] onPing: " + message);
                    return java.net.http.WebSocket.Listener.super.onPing(webSocket, message);
                }

                @Override
                public CompletionStage<?> onPong(java.net.http.WebSocket webSocket, ByteBuffer message) {
                    LOG.debug("WebSocket[" + name + "] onPong: " + message);
                    return java.net.http.WebSocket.Listener.super.onPong(webSocket, message);
                }

                @Override
                public CompletionStage<?> onClose(java.net.http.WebSocket webSocket, int statusCode, String reason) {
                    LOG.info("WebSocket[" + name + "] onClosed. statusCode: " + statusCode + ", reason: " + reason);
                    this.isClosed = true;
                    return java.net.http.WebSocket.Listener.super.onClose(webSocket, statusCode, reason);
                }

                @Override
                public void onError(java.net.http.WebSocket webSocket, Throwable error) {
                    final String message = "WebSocket[" + name + "] onError. cause: " + error.getMessage();
                    LOG.error(message);
                    this.isClosed = true;
                    java.net.http.WebSocket.Listener.super.onError(webSocket, error);
                }
                public List<KV<Instant, String>> getMessages() {
                    return new ArrayList<>(this.messages);
                }

                public void clearMessages() {
                    this.messages.clear();
                }

                public boolean isClosed() {
                    return this.isClosed;
                }
            }

        }

        private static class ConvertTradeDoFn<InputSchemaT,RuntimeSchemaT,T> extends DoFn<KV<String, List<KV<Instant, String>>>, T> {

            private final String name;
            private final Market market;
            private final AssetType assetType;

            private final Market.TradeCreator<RuntimeSchemaT,T> creator;
            private final SchemaUtil.SchemaConverter<InputSchemaT,RuntimeSchemaT> schemaConverter;
            private final InputSchemaT tradeSchema;


            private final TupleTag<T> outputTag;
            private final TupleTag<T> failuresTag;

            private transient RuntimeSchemaT runtimeTradeSchema;


            ConvertTradeDoFn(
                    final String name,
                    final Market market,
                    final AssetType assetType,
                    final Market.TradeCreator<RuntimeSchemaT,T> creator,
                    final SchemaUtil.SchemaConverter<InputSchemaT,RuntimeSchemaT> schemaConverter,
                    final InputSchemaT tradeSchema,
                    final TupleTag<T> outputTag,
                    final TupleTag<T> failuresTag) {

                this.name = name;
                this.market = market;
                this.assetType = assetType;

                this.creator = creator;
                this.schemaConverter = schemaConverter;
                this.tradeSchema = tradeSchema;

                this.outputTag = outputTag;
                this.failuresTag = failuresTag;
            }

            @Setup
            public void setup() {
                this.runtimeTradeSchema = schemaConverter.convert(tradeSchema);
            }

            @ProcessElement
            public void processElement(final ProcessContext c) {
                final List<KV<Instant, String>> messages = c.element().getValue();
                try {
                    final List<T> trades = market.parseTrade(runtimeTradeSchema, creator, assetType, messages);
                    for (final T trade : trades) {
                        c.output(outputTag, trade);
                    }
                } catch (Throwable e) {
                    LOG.error("failed parse element. name: " + name + ", eventType: trade, assetType: " + assetType + ", messages: " + messages + ". cause: " + e.getMessage());
                    throw e;
                }
            }

        }

        private static class ConvertOrderBooksDoFn<InputSchemaT,RuntimeSchemaT,T> extends DoFn<KV<String, List<KV<Instant, String>>>, T> {
            private static final String STATE_ID_ORDER_BOOKS_SIZES = "orderBooksSizesState";

            @StateId(STATE_ID_ORDER_BOOKS_SIZES)
            private final StateSpec<ValueState<Map<Double,Double>>> orderBooksSizesStateSpec = StateSpecs
                    .value(MapCoder.of(DoubleCoder.of(), DoubleCoder.of()));

            private final String name;
            private final Market market;
            private final AssetType assetType;

            private final Market.OrderBooksCreator<RuntimeSchemaT,T> creator;
            private final SchemaUtil.SchemaConverter<InputSchemaT,RuntimeSchemaT> schemaConverter;
            private final InputSchemaT orderBooksSchema;

            private final TupleTag<T> outputTag;
            private final TupleTag<T> failuresTag;

            private transient RuntimeSchemaT runtimeOrderBooksSchema;


            ConvertOrderBooksDoFn(
                    final String name,
                    final Market market,
                    final AssetType assetType,
                    final Market.OrderBooksCreator<RuntimeSchemaT,T> creator,
                    final SchemaUtil.SchemaConverter<InputSchemaT,RuntimeSchemaT> schemaConverter,
                    final InputSchemaT orderBooksSchema,
                    final TupleTag<T> outputTag,
                    final TupleTag<T> failuresTag) {

                this.name = name;
                this.market = market;
                this.assetType = assetType;

                this.creator = creator;
                this.schemaConverter = schemaConverter;
                this.orderBooksSchema = orderBooksSchema;

                this.outputTag = outputTag;
                this.failuresTag = failuresTag;
            }

            @Setup
            public void setup() {
                this.runtimeOrderBooksSchema = schemaConverter.convert(orderBooksSchema);
            }

            @ProcessElement
            public void processElement(
                    final ProcessContext c,
                    final @StateId(STATE_ID_ORDER_BOOKS_SIZES) ValueState<Map<Double, Double>> orderBooksSizesState) {

                final String symbol = c.element().getKey();
                final List<KV<Instant, String>> messages = c.element().getValue();
                try {
                    final Map<Double, Double> orderBooksSizes = Optional.ofNullable(orderBooksSizesState.read()).orElseGet(HashMap::new);
                    final KV<T, Map<Double, Double>> parsed = market.parseOrderBooks(runtimeOrderBooksSchema, creator, symbol, assetType, messages, orderBooksSizes);
                    c.output(outputTag, parsed.getKey());
                    orderBooksSizesState.write(parsed.getValue());
                } catch (Throwable e) {
                    LOG.error("failed parse element. name: " + name + ", eventType: orderbooks, assetType: " + assetType + ", messages: " + messages + ". cause: " + e.getMessage());
                    throw e;
                }
            }

        }

    }

