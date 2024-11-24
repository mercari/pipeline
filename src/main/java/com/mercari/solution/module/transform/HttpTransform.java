package com.mercari.solution.module.transform;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.mercari.solution.module.*;
import com.mercari.solution.util.DateTimeUtil;
import com.mercari.solution.util.TemplateUtil;
import com.mercari.solution.util.coder.ElementCoder;
import com.mercari.solution.util.coder.UnionMapCoder;
import com.mercari.solution.util.schema.converter.JsonToMapConverter;
import com.mercari.solution.util.pipeline.OptionUtil;
import com.mercari.solution.util.pipeline.Select;
import com.mercari.solution.util.pipeline.Union;
import com.mercari.solution.util.pipeline.Views;
import com.mercari.solution.util.pipeline.select.SelectFunction;
import com.mercari.solution.util.schema.converter.JsonToElementConverter;
import freemarker.template.Template;
import org.apache.beam.io.requestresponse.*;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.*;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Collectors;


@Transform.Module(name="http")
public class HttpTransform extends Transform {

    private static final Logger LOG = LoggerFactory.getLogger(HttpTransform.class);

    private static final String METADATA_ENDPOINT = "http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/";

    public static class HttpTransformParameters implements Serializable {

        private RequestParameters request;
        private ResponseParameters response;
        private RetryParameters retry;
        private Integer timeoutSecond;
        private JsonArray select;
        private JsonElement filter;
        private String flattenField;

        public void validate() {
            final List<String> errorMessages = new ArrayList<>();
            if(this.request == null) {
                errorMessages.add("parameters.request must not be null.");
            } else {
                errorMessages.addAll(this.request.validate());
            }

            if(this.response == null) {
                errorMessages.add("parameters.response must not be null.");
            } else {
                errorMessages.addAll(this.response.validate());
            }

            if(this.retry != null) {
                errorMessages.addAll(this.retry.validate());
            }

            if(!errorMessages.isEmpty()) {
                throw new IllegalModuleException(errorMessages);
            }
        }

        public void setDefaults() {
            this.request.setDefaults();
            this.response.setDefaults();
            if(this.retry == null) {
                this.retry = new RetryParameters();
            }
            this.retry.setDefaults();
            if(this.timeoutSecond == null) {
                this.timeoutSecond = 30;
            }
        }

        private static class RequestParameters implements Serializable {

            private String endpoint;
            private String method;
            private Map<String,String> params;
            private JsonElement body;
            private Map<String, String> headers;

            // prepare requests
            //private String name;
            //private String path;
            private List<RequestParameters> prepareRequests;

            private transient Template templateEndpoint;
            private transient Map<String,Template> templateParams;
            private transient Map<String,Template> templateHeaders;
            private transient Template templateBody;

            private transient Map<String, String> prepareParameters;

            public List<String> validate() {
                final List<String> errorMessages = new ArrayList<>();

                if(this.endpoint == null) {
                    errorMessages.add("parameters.request.endpoint must not be null.");
                }

                return errorMessages;
            }

            public void setDefaults() {
                if(this.method == null) {
                    this.method = "GET";
                }
                if(this.params == null) {
                    this.params = new HashMap<>();
                }
                if(this.headers == null) {
                    this.headers = new HashMap<>();
                }
                if(this.prepareRequests == null) {
                    this.prepareRequests = new ArrayList<>();
                }
                for(final RequestParameters prepareRequest : prepareRequests) {
                    prepareRequest.setDefaults();
                }
            }

            public void setup() {
                this.templateEndpoint = TemplateUtil.createStrictTemplate("TemplateEndpoint", endpoint);
                this.templateParams = new HashMap<>();
                if(!this.params.isEmpty()) {
                    int count = 0;
                    for(final Map.Entry<String, String> entry : params.entrySet()) {
                        final Template template = TemplateUtil.createStrictTemplate("TemplateParams" + count, entry.getValue());
                        this.templateParams.put(entry.getKey(), template);
                        count++;
                    }
                }
                this.templateHeaders = new HashMap<>();
                if(!this.headers.isEmpty()) {
                    int count = 0;
                    for(final Map.Entry<String, String> entry : headers.entrySet()) {
                        final Template template = TemplateUtil.createStrictTemplate("TemplateHeaders" + count, entry.getValue());
                        this.templateHeaders.put(entry.getKey(), template);
                        count++;
                    }
                }
                if(this.body != null) {
                    this.templateBody = TemplateUtil.createStrictTemplate("TemplateBody", this.body.toString());
                }

                for(final RequestParameters prepareRequest : prepareRequests) {
                    prepareRequest.setup();
                }

                //this.prepareParameters = getPrepareParameters();
            }

            /*
            public Map<String, String> getPrepareParameters() {
                final Map<String, String> prepareParameters = new HashMap<>();
                if(prepareRequests.isEmpty()) {
                    return prepareParameters;
                }
                final Map<String, Object> params = new HashMap<>();
                TemplateUtil.setFunctions(params);
                try(final HttpClient client = HttpClient.newBuilder().build()) {
                    for(final RequestParameters prepareRequest : prepareRequests) {
                        final HttpResponse<String> httpResponse = sendRequest(client, prepareRequest, params, HttpResponse.BodyHandlers.ofString());
                        final String prepareParameter = prepareRequest.getParameter(httpResponse.body());
                        prepareParameters.put(prepareRequest.name, prepareParameter);
                        params.put(prepareRequest.name, prepareParameter);
                    }
                    return prepareParameters;
                } catch (Exception e) {
                    throw new RuntimeException("Failed to prepare parameter request: " + prepareRequests, e);
                }
            }

             */

            /*
            private String getParameter(String body) {
                if(path == null) {
                    return body;
                } else {
                    try {
                        final Object str = org.apache.beam.vendor.calcite.v1_28_0.com.jayway.jsonpath.JsonPath.read(body, path);
                        return switch (str) {
                            case String s -> s;
                            case Object n -> n.toString();
                        };
                    } catch (Throwable e) {
                        throw new RuntimeException("failed to get jsonPath: " + path + " from response body: " + body, e);
                    }
                }
            }

             */

        }

        private static class ResponseParameters implements Serializable {

            private HttpTransform.Format format;
            private JsonElement schema;
            private List<Integer> acceptableStatusCodes;

            public List<String> validate() {
                final List<String> errorMessages = new ArrayList<>();
                if(this.format == null) {
                    errorMessages.add("parameters.response.format must not be null.");
                }
                if(this.schema == null || !this.schema.isJsonObject()) {
                    errorMessages.add("parameters.response.schema must not be null.");
                }
                if(this.acceptableStatusCodes != null) {
                    for(final Integer acceptableStatusCode : acceptableStatusCodes) {
                        if(acceptableStatusCode == null) {
                            errorMessages.add("parameters.response.acceptableStatusCodes must not be null.");
                        } else if(acceptableStatusCode >= 600 || acceptableStatusCode < 100) {
                            errorMessages.add("parameters.response.acceptableStatusCodes value[" + acceptableStatusCode + "] must be between 100 and 599");
                        }
                    }
                }
                return errorMessages;
            }

            public void setDefaults() {
                if(this.acceptableStatusCodes == null) {
                    this.acceptableStatusCodes = new ArrayList<>();
                }
            }

        }

        private static class RetryParameters implements Serializable {

            private BackoffParameters backoff;

            public List<String> validate() {
                return new ArrayList<>();
            }

            public void setDefaults() {
                if(backoff == null) {
                    backoff = new BackoffParameters();
                }
                backoff.setDefaults();
            }

        }

        private static class BackoffParameters implements Serializable {

            private Double exponent;
            private Integer initialBackoffSecond;
            private Integer maxBackoffSecond;
            private Integer maxCumulativeBackoffSecond;
            private Integer maxRetries;

            public List<String> validate(String name) {
                return new ArrayList<>();
            }

            public void setDefaults() {
                // reference: FluentBackoff.DEFAULT
                if(exponent == null) {
                    exponent = 1.5;
                }
                if(initialBackoffSecond == null) {
                    initialBackoffSecond = 1;
                }
                if(maxBackoffSecond == null) {
                    maxBackoffSecond = 60 * 60 * 24 * 1000;
                }
                if(maxCumulativeBackoffSecond == null) {
                    maxCumulativeBackoffSecond = 60 * 60 * 24 * 1000;
                }
                if(maxRetries == null) {
                    this.maxRetries = Integer.MAX_VALUE;
                }
            }

        }

    }

    public enum Format {
        text,
        bytes,
        json
    }

    @Override
    public MCollectionTuple expand(MCollectionTuple inputs) {
        final HttpTransformParameters parameters = getParameters(HttpTransformParameters.class);
        parameters.validate();
        parameters.setDefaults();

        final Schema responseSchema = createResponseSchema(parameters.response);
        final List<SelectFunction> selectFunctions = SelectFunction.of(parameters.select, responseSchema.getFields());
        final Schema outputSchema = createOutputSchema(responseSchema, selectFunctions, parameters.flattenField);

        final PCollection<MElement> elements = inputs
                .apply("Union", Union.flatten()
                        .withWaits(getWaits())
                        .withStrategy(getStrategy()));
        final Schema inputSchema = Union.createUnionSchema(inputs);

        final PCollection<KV<Map<String, Object>, MElement>> elementWithParams;
        if(parameters.request.prepareRequests != null && false) {
            final PCollection<Long> seed;
            if(OptionUtil.isStreaming(inputs)) {
                seed = inputs.getPipeline().begin()
                        .apply("Seed", Create
                                .of(1L)
                                .withCoder(VarLongCoder.of()))
                        .apply("WithTrigger", Window
                                .<Long>into(new GlobalWindows())
                                .triggering(Repeatedly.forever(AfterProcessingTime.pastFirstElementInPane()))
                                .discardingFiredPanes());
            } else {
                seed = inputs.getPipeline().begin()
                        .apply("Seed", Create
                                .of(1L)
                                .withCoder(VarLongCoder.of()));
            }
            final PCollectionView<Map<String, Object>> params = seed
                    .apply("Prepare", ParDo.of(new PrepareDoFn(getParametersText())))
                    .setCoder(UnionMapCoder.mapCoder())
                    .apply(View.asSingleton());
            elementWithParams = elements
                    .apply("Lookup", ParDo.of(new LookupDoFn(params)).withSideInputs(params));
        } else if(!getSideInputs().isEmpty()) {
            final Map<String, PCollectionView<?>> views = new HashMap<>();
            for(final MCollection collection : getSideInputs()) {
                final PCollectionView<Map<String,Object>> sideInputView = collection
                        .apply("AsView" + collection.getName(), Views.singleton());
                views.put(collection.getName(), sideInputView);
            }
            elementWithParams = elements
                    .apply("Lookup", ParDo.of(new Lookup2DoFn(views)).withSideInputs(views));
        } else {
            elementWithParams = elements
                    .apply("Lookup", WithKeys.of(new HashMap<>()));
        }

        final HttpCaller caller = new HttpCaller(
                getName(), getParametersText(), inputSchema, selectFunctions, responseSchema, outputSchema);
        final RequestResponseIO<KV<Map<String, Object>, MElement>, MElement> requestResponseIO = RequestResponseIO
                .ofCallerAndSetupTeardown(caller, ElementCoder.of(outputSchema))
                .withTimeout(Duration.standardSeconds(parameters.timeoutSecond));

        final Result<MElement> httpResult = elementWithParams
                .setCoder(KvCoder.of(UnionMapCoder.mapCoder(), ElementCoder.of(inputSchema)))
                .apply("HttpCall", requestResponseIO);

        PCollectionList<MElement> errorList = PCollectionList.empty(inputs.getPipeline());
        final PCollection<MElement> errors = httpResult.getFailures()
                .apply("Failures", ParDo.of(new FailureDoFn(getJobName(), getName(), getFailFast())));
        errorList = errorList.and(errors);

        final PCollection<MElement> output;
        if(selectFunctions.isEmpty()) {
            output = httpResult.getResponses();
        } else {
            final String filterString = Optional
                    .ofNullable(parameters.filter)
                    .map(JsonElement::toString)
                    .orElse(null);
            final Select.Transform selectTransform = Select.of(
                    getJobName(), getName(), filterString, selectFunctions, parameters.flattenField, getFailFast(), getOutputFailure(), DataType.ELEMENT);
            final PCollectionTuple tuple = httpResult.getResponses()
                    .apply("Select", selectTransform);
            output = tuple.get(selectTransform.outputTag);
            errorList = errorList.and(tuple.get(selectTransform.failuresTag));
        }

        return MCollectionTuple
                .of(output, outputSchema)
                .failure(errorList.apply(Flatten.pCollections()));
    }

    private static Schema createResponseSchema(final HttpTransformParameters.ResponseParameters response) {
        final Schema.FieldType fieldType;
        if(response.schema == null) {
            fieldType = switch (response.format) {
                case bytes -> Schema.FieldType.BYTES.withNullable(true);
                case text -> Schema.FieldType.STRING.withNullable(true);
                case json -> Schema.FieldType.JSON.withNullable(true);
            };
        } else {
            final Schema responseSchema = Schema.parse(response.schema.getAsJsonObject());
            fieldType = Schema.FieldType.element(responseSchema);
        }

        return Schema.builder()
                .withField("statusCode", Schema.FieldType.INT32)
                .withField("body", fieldType)
                .withField("headers", Schema.FieldType.map(Schema.FieldType.array(Schema.FieldType.STRING)).withNullable(true))
                .build();
    }

    private static Schema createOutputSchema(
            final Schema responseSchema,
            final List<SelectFunction> selectFunctions,
            final String flattenField) {

        if(selectFunctions.isEmpty()) {
            return responseSchema;
        } else {
            return SelectFunction.createSchema(selectFunctions, flattenField);
        }
    }

    private static class HttpCaller implements Caller<KV<Map<String, Object>, MElement>, MElement>, SetupTeardown {

        private final String name;
        private final String parametersText;
        private final Schema inputSchema;

        private final Schema responseSchema;
        private final Schema outputSchema;

        private final List<SelectFunction> selectFunctions;

        private final Set<HttpCaller.TokenType> tokenTypes;

        private transient HttpClient client;

        private transient String idToken;
        private transient String accessToken;

        private transient HttpTransformParameters.RequestParameters request;
        private transient HttpTransformParameters.ResponseParameters response;
        private transient HttpTransformParameters.RetryParameters retry;
        private transient Integer timeoutSecond;


        public HttpCaller(
                final String name,
                final String parametersText,
                final Schema inputSchema,
                final List<SelectFunction> selectFunctions,
                final Schema responseSchema,
                final Schema outputSchema) {

            this.name = name;
            this.parametersText = parametersText;
            this.inputSchema = inputSchema;
            this.selectFunctions = selectFunctions;

            this.responseSchema = responseSchema;
            this.outputSchema = outputSchema;

            this.tokenTypes = new HashSet<>();
            /*
            this.tokenTypes = request.headers.entrySet().stream().map(
                            s -> {
                                if(!s.getKey().contains("Authorization")) {
                                    return HttpCaller.TokenType.none;
                                } else if(s.getValue().contains("_id_token")) {
                                    return HttpCaller.TokenType.id;
                                } else if(s.getValue().contains("_access_token")) {
                                    return HttpCaller.TokenType.access;
                                } else {
                                    return HttpCaller.TokenType.none;
                                }
                            })
                    .collect(Collectors.toSet());

             */
        }

        @Override
        public void setup() throws UserCodeExecutionException {

            this.client = HttpClient.newBuilder().build();

            final HttpTransformParameters parameters = new Gson().fromJson(parametersText, HttpTransformParameters.class);
            parameters.setDefaults();

            this.request = parameters.request;
            this.response = parameters.response;
            this.retry = parameters.retry;
            this.timeoutSecond = parameters.timeoutSecond;

            this.request.setup();

            if(this.tokenTypes.contains(HttpCaller.TokenType.id)) {
                this.idToken = getIdToken(client, request.endpoint);
            }
            if(this.tokenTypes.contains(HttpCaller.TokenType.access)) {
                this.accessToken = getAccessToken(client);
            }

            for(final SelectFunction selectFunction : selectFunctions) {
                selectFunction.setup();
            }

            this.outputSchema.setup();
        }

        @Override
        public void teardown() {

        }

        @Override
        public MElement call(KV<Map<String, Object>, MElement> kv) throws UserCodeExecutionException {
            try {
                HttpResponse.BodyHandler<?> bodyHandler = switch (response.format) {
                    case text, json -> HttpResponse.BodyHandlers.ofString();
                    case bytes -> HttpResponse.BodyHandlers.ofByteArray();
                };

                final MElement element = kv.getValue();
                final Map<String, Object> params = element.asStandardMap(inputSchema, null);
                final Map<String, Object> preparedParams = kv.getKey();
                if(preparedParams != null) {
                    params.putAll(preparedParams);
                }
                TemplateUtil.setFunctions(params);

                HttpResponse<?> httpResponse = sendRequest(client, request, params, bodyHandler);
                if(httpResponse.statusCode() == 401
                        && (tokenTypes.contains(TokenType.id) || tokenTypes.contains(TokenType.access))) {

                    if(tokenTypes.contains(TokenType.id)) {
                        LOG.info("Try to reacquire id token");
                        this.idToken = getIdToken(client, request.endpoint);
                    }
                    if(tokenTypes.contains(TokenType.access)) {
                        LOG.info("Try to reacquire access token");
                        this.accessToken = getAccessToken(client);
                    }
                    httpResponse = sendRequest(client, request, params, bodyHandler);
                }

                final boolean acceptable = response.acceptableStatusCodes.contains(httpResponse.statusCode());
                if(httpResponse.statusCode() >= 400 && httpResponse.statusCode() < 500) {
                    if(!acceptable) {
                        final String errorMessage = "Illegal response code: " + httpResponse.statusCode() + ", for endpoint: " + request.endpoint + ", response: " + httpResponse.body();
                        LOG.error(errorMessage);
                        throw new UserCodeExecutionException(errorMessage);
                    } else {
                        LOG.info("Acceptable code: {}", httpResponse.statusCode());
                    }
                }

                final Map<String, Object> output = switch (this.response.format) {
                    case text -> {
                        final String body = (String) httpResponse.body();
                        final Map<String, Object> values = new HashMap<>();
                        values.put("statusCode", httpResponse.statusCode());
                        values.put("body", body);
                        values.put("headers", httpResponse.headers().map());
                        values.put("timestamp", DateTimeUtil.toEpochMicroSecond(java.time.Instant.now()));
                        yield values;
                    }
                    case bytes -> {
                        final byte[] body = (byte[]) httpResponse.body();
                        final Map<String, Object> values = new HashMap<>();
                        values.put("statusCode", httpResponse.statusCode());
                        values.put("body", ByteBuffer.wrap(body));
                        values.put("headers", httpResponse.headers().map());
                        values.put("timestamp", DateTimeUtil.toEpochMicroSecond(java.time.Instant.now()));
                        yield values;
                    }
                    case json -> {
                        final String body = (String) httpResponse.body();
                        JsonElement responseJson = new Gson().fromJson(body, JsonElement.class);
                        if(!responseJson.isJsonObject()) {
                            responseJson = new JsonObject();
                        }

                        final JsonObject jsonObject = new JsonObject();
                        jsonObject.addProperty("statusCode", httpResponse.statusCode());
                        jsonObject.addProperty("timestamp", Instant.now().toString());
                        final JsonObject headers = new JsonObject();
                        for(final Map.Entry<String, List<String>> entry : httpResponse.headers().map().entrySet()) {
                            final JsonArray headerValues = new JsonArray();
                            entry.getValue().forEach(headerValues::add);
                            headers.add(entry.getKey(), headerValues);
                        }
                        jsonObject.add("headers", headers);
                        if(!responseSchema.hasField("body")) {
                            throw new RuntimeException("Not found body");
                        }
                        switch (responseSchema.getField("body").getFieldType().getType()) {
                            case element, map, json -> {
                                jsonObject.add("body", responseJson.getAsJsonObject());
                            }
                            default -> {
                                jsonObject.addProperty("body", responseJson.toString());
                            }
                        }
                        yield JsonToElementConverter.convert(responseSchema, jsonObject);
                    }
                };
                return MElement.of(output, element.getEpochMillis());
            } catch (URISyntaxException e) {
                throw new UserCodeExecutionException("Illegal endpoint: " + request.endpoint, e);
            } catch (IOException | InterruptedException e) {
                throw new UserCodeRemoteSystemException("Remote error: ", e);
            } catch (Throwable e) {
                throw new UserCodeExecutionException("Failed to send http request.", e);
            }
        }

        public enum TokenType {
            id,
            access,
            none
        }
    }

    private static class PrepareDoFn extends DoFn<Long, Map<String, Object>> {

        private final String parametersText;
        private transient HttpTransformParameters parameters;

        PrepareDoFn(final String parametersText) {
            this.parametersText = parametersText;
        }

        @Setup
        public void setup() {
            this.parameters = new Gson().fromJson(parametersText, HttpTransformParameters.class);
            this.parameters.setDefaults();
            this.parameters.request.setup();
        }

        @ProcessElement
        public void processElement(ProcessContext c) {
            final Map<String, Object> values = new HashMap<>();
            try {
                final HttpClient client = HttpClient.newBuilder().build();
                for(var request : parameters.request.prepareRequests) {
                    final HttpResponse<String> response = sendRequest(
                            client, request, values, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
                    final String body = response.body();
                    JsonElement responseJson = new Gson().fromJson(body, JsonElement.class);
                    if(!responseJson.isJsonObject()) {
                        responseJson = new JsonObject();
                    }
                    final Map<String, Object> map = JsonToMapConverter.convert(responseJson);
                    values.putAll(map);
                }
            } catch (final Throwable e) {
                System.out.println(e);
            }
            c.output(values);
        }

    }

    private static class LookupDoFn extends DoFn<MElement, KV<Map<String, Object>, MElement>> {

        private final PCollectionView<Map<String, Object>> view;

        LookupDoFn(final PCollectionView<Map<String, Object>> view) {
            this.view = view;
        }

        @Setup
        public void setup() {

        }

        @ProcessElement
        public void processElement(ProcessContext c) {
            final Map<String, Object> sideInput = c.sideInput(view);
            c.output(KV.of(sideInput, c.element()));
        }

    }

    private static class Lookup2DoFn extends DoFn<MElement, KV<Map<String, Object>, MElement>> {

        private final Map<String, PCollectionView<?>> views;

        Lookup2DoFn(final Map<String, PCollectionView<?>> views) {
            this.views = views;
        }

        @Setup
        public void setup() {

        }

        @ProcessElement
        public void processElement(ProcessContext c) {
            final Map<String, Object> sideInputs = new HashMap<>();
            for(final PCollectionView<?> view : views.values()) {
                final Map<String, Object> sideInput = (Map<String, Object>) c.sideInput(view);
                if(sideInput != null) {
                    sideInputs.putAll(sideInput);
                }
            }
            System.out.println(sideInputs);
            c.output(KV.of(sideInputs, c.element()));
        }

    }

    private static class FailureDoFn extends DoFn<ApiIOError, MElement> {

        private final String jobName;
        private final String name;
        private final boolean failFast;

        FailureDoFn(final String jobName, final String name, final boolean failFast) {
            this.jobName = jobName;
            this.name = name;
            this.failFast = failFast;
        }

        @ProcessElement
        public void processElement(ProcessContext c) {
            final ApiIOError error = c.element();
            if(error == null) {
                return;
            }

            final JsonObject output = new JsonObject();
            output.addProperty("message", error.getMessage());
            output.addProperty("stackTrace", error.getStackTrace());
            output.addProperty("observedTimestamp", error.getObservedTimestamp().toString());
            final MFailure failure = MFailure.of(jobName, name, error.getRequestAsString(), output.toString(), c.timestamp());
            LOG.error("failure: {}", output);
            c.output(failure.toElement(c.timestamp()));

            if(failFast) {
                throw new IllegalArgumentException("Failed to http transform: " + output);
            }
        }

    }

    private static <ResponseT> HttpResponse<ResponseT> sendRequest(
            final HttpClient client,
            final HttpTransformParameters.RequestParameters request,
            final Map<String, Object> values,
            final HttpResponse.BodyHandler<ResponseT> bodyHandler) throws IOException, InterruptedException, URISyntaxException {

        final String url = createEndpoint(request, values);
        final String bodyText;
        if(request.templateBody != null) {
            bodyText = TemplateUtil.executeStrictTemplate(request.templateBody, values);
        } else {
            bodyText = "";
        }
        final HttpRequest.BodyPublisher bodyPublisher = HttpRequest.BodyPublishers.ofString(bodyText);

        HttpRequest.Builder builder = HttpRequest.newBuilder()
                .uri(new URI(url))
                .timeout(java.time.Duration.ofSeconds(100))
                .method(request.method.toUpperCase(), bodyPublisher);

        for(final Map.Entry<String, Template> entry : request.templateHeaders.entrySet()) {
            final String headerValue = TemplateUtil.executeStrictTemplate(entry.getValue(), values);
            builder = builder.header(entry.getKey(), headerValue);
        }

        return client.send(builder.build(), bodyHandler);
    }

    private static String createEndpoint(
            final HttpTransformParameters.RequestParameters request,
            final Map<String, Object> values) {

        final String params = request.templateParams.entrySet()
                .stream()
                .map(e -> e.getKey() + "=" + URLEncoder
                        .encode(TemplateUtil
                                .executeStrictTemplate(e.getValue(), values), StandardCharsets.UTF_8))
                .collect(Collectors.joining("&"));

        return TemplateUtil.executeStrictTemplate(request.templateEndpoint, values) + (params.isEmpty() ? "" : ("?" + params));
    }

    private static String getIdToken(
            final HttpClient client,
            final String endpoint) throws UserCodeExecutionException {

        try {
            final HttpRequest httpRequest = HttpRequest.newBuilder()
                    .uri(new URI(METADATA_ENDPOINT + "identity?audience=" + endpoint))
                    .header("Metadata-Flavor", "Google")
                    .GET()
                    .build();
            final HttpResponse<String> httpResponse = client
                    .send(httpRequest, HttpResponse.BodyHandlers.ofString());
            LOG.info("Succeed to get id token");
            return httpResponse.body();
        } catch (URISyntaxException e) {
            throw new UserCodeExecutionException("Failed to get id token", e);
        } catch (Throwable e) {
            LOG.error("Failed to get id token cause: {}", e.getMessage());
            throw new UserCodeRemoteSystemException("Failed to get id token", e);
        }
    }

    private static String getAccessToken(final HttpClient client) throws UserCodeExecutionException {
        try {
            final HttpRequest httpRequest = HttpRequest.newBuilder()
                    .uri(new URI(METADATA_ENDPOINT + "token"))
                    .header("Metadata-Flavor", "Google")
                    .GET()
                    .build();
            final HttpResponse<String> httpResponse = client
                    .send(httpRequest, HttpResponse.BodyHandlers.ofString());
            final JsonElement responseJson = new Gson().fromJson(httpResponse.body(), JsonElement.class);
            if(!responseJson.isJsonObject()) {
                throw new IllegalStateException("Illegal token response: " + responseJson);
            }
            final JsonObject jsonObject = responseJson.getAsJsonObject();
            if(!jsonObject.has("access_token")) {
                throw new IllegalStateException("Illegal token response: " + responseJson);
            }

            LOG.info("Succeed to get access token");
            return jsonObject.get("access_token").getAsString();
        } catch (final URISyntaxException e) {
            throw new UserCodeExecutionException("Failed to get access token", e);
        } catch (final Throwable e) {
            LOG.error("Failed to get access token cause: {}", e.getMessage());
            throw new UserCodeRemoteSystemException("Failed to get access token", e);
        }
    }

}
