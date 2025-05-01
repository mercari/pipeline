package com.mercari.solution.module.transform;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.mercari.solution.module.*;
import com.mercari.solution.util.DateTimeUtil;
import com.mercari.solution.util.coder.ElementCoder;
import com.mercari.solution.util.coder.UnionMapCoder;
import com.mercari.solution.util.domain.web.HttpUtil;
import com.mercari.solution.util.pipeline.*;
import com.mercari.solution.util.pipeline.select.SelectFunction;
import com.mercari.solution.util.schema.converter.JsonToElementConverter;
import org.apache.beam.io.requestresponse.*;
import org.apache.beam.sdk.coders.KvCoder;
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
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.util.*;


@Transform.Module(name="http")
public class HttpTransform extends Transform {

    private static final Logger LOG = LoggerFactory.getLogger(HttpTransform.class);

    public static class Parameters implements Serializable {

        //private Auth.Parameters auth;
        private HttpUtil.Request request;
        private List<HttpUtil.Request> requests;
        private HttpUtil.Response response;
        private RetryParameters retry;
        private Integer timeoutSecond;
        private JsonArray select;
        private JsonElement filter;
        private String flattenField;

        public void validate() {
            final List<String> errorMessages = new ArrayList<>();
            if(this.request == null && (this.requests == null || this.requests.isEmpty())) {
                errorMessages.add("parameters.request must not be null.");
            } else if(this.request != null) {
                errorMessages.addAll(this.request.validate(""));
            } else {
                for(var req : requests) {
                    req.validate("");
                }
            }

            if(this.response == null) {
                errorMessages.add("parameters.response must not be null.");
            } else {
                errorMessages.addAll(this.response.validate(""));
            }

            if(this.retry != null) {
                errorMessages.addAll(this.retry.validate());
            }

            //if(auth != null) {
            //    errorMessages.addAll(this.auth.validate(""));
            //}

            if(!errorMessages.isEmpty()) {
                throw new IllegalModuleException(errorMessages);
            }
        }

        public void setDefaults() {
            if(requests == null) {
                requests = new ArrayList<>();
            }
            if(request != null) {
                requests.add(request);
            }
            for(var req : requests) {
                req.setDefaults();
            }
            this.response.setDefaults();
            if(this.retry == null) {
                this.retry = new RetryParameters();
            }
            this.retry.setDefaults();
            if(this.timeoutSecond == null) {
                this.timeoutSecond = 30;
            }
        }


        /*
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

         */

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


        }
             */

        /*
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

         */

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
    public MCollectionTuple expand(
            final MCollectionTuple inputs,
            final MErrorHandler errorHandler) {

        final Parameters parameters = getParameters(Parameters.class);
        parameters.validate();
        parameters.setDefaults();

        final Schema responseSchema = HttpUtil.createResponseSchema(parameters.response);
        final List<SelectFunction> selectFunctions = SelectFunction.of(parameters.select, responseSchema.getFields());
        final Schema outputSchema = createOutputSchema(responseSchema, selectFunctions, parameters.flattenField);

        final PCollection<MElement> input = inputs
                .apply("Union", Union.flatten()
                        .withWaits(getWaits())
                        .withStrategy(getStrategy()));
        final Schema inputSchema = Union.createUnionSchema(inputs);

        final PCollection<KV<Map<String, Object>, MElement>> inputWithParams;
        /*
        if(parameters.auth != null || !getSideInputs().isEmpty()) {

            final Map<String, PCollectionView<?>> views = new HashMap<>();
            for(final MCollection collection : getSideInputs()) {
                final PCollectionView<Map<String,Object>> sideInputView = collection
                        .apply("AsView" + collection.getName(), Views.singleton());
                views.put(collection.getName(), sideInputView);
            }

            if(parameters.auth != null) {
                final Auth.Transform transform = Auth.of(
                        getJobName(), getName(), parameters.auth, getFailFast(), getLoggings());
                final PCollectionTuple auth = inputs.getPipeline().begin()
                        .apply("Auth", transform);

                final PCollection<Map<String, Object>> authOutput = auth
                        .get(transform.outputTag)
                        .apply("WithTrigger", Window
                                .<Map<String, Object>>into(new GlobalWindows())
                                .triggering(Repeatedly.forever(AfterProcessingTime.pastFirstElementInPane()))
                                .discardingFiredPanes());
                final PCollectionView<Map<String, Object>> params = authOutput.apply(View.asSingleton());
                views.put("auth", params);
            }
            inputWithParams = input
                    .apply("LookupAuth", ParDo.of(new LookupDoFn(views)).withSideInputs(views));
        } else {
            inputWithParams = input
                    .apply("EmptyAuth", WithKeys.of(new HashMap<>()));
        }

         */

        inputWithParams = input
                .apply("EmptyAuth", WithKeys.of(new HashMap<>()));

        final HttpCaller caller = new HttpCaller(
                getName(), getParametersText(), inputSchema, selectFunctions, responseSchema, outputSchema, getLoggings());
        final RequestResponseIO<KV<Map<String, Object>, MElement>, MElement> requestResponseIO = RequestResponseIO
                .ofCallerAndSetupTeardown(caller, ElementCoder.of(outputSchema))
                .withTimeout(Duration.standardSeconds(parameters.timeoutSecond));

        final Result<MElement> httpResult = inputWithParams
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
                    getJobName(), getName(), filterString, selectFunctions, parameters.flattenField, getLoggings(), getFailFast(), DataType.ELEMENT);
            final PCollectionTuple tuple = httpResult.getResponses()
                    .apply("Select", selectTransform);
            output = tuple.get(selectTransform.outputTag);
            if(getOutputFailure()) {
                errorList = errorList.and(tuple.get(selectTransform.failuresTag));
            }
        }

        return MCollectionTuple
                .of(output, outputSchema);
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

        private final Map<String, Logging> loggings;

        private final Set<HttpCaller.TokenType> tokenTypes;

        private transient HttpClient client;

        private transient HttpUtil.Request request;
        private transient HttpUtil.Response response;
        private transient Parameters.RetryParameters retry;
        private transient Integer timeoutSecond;


        public HttpCaller(
                final String name,
                final String parametersText,
                final Schema inputSchema,
                final List<SelectFunction> selectFunctions,
                final Schema responseSchema,
                final Schema outputSchema,
                final List<Logging> loggings) {

            this.name = name;
            this.parametersText = parametersText;
            this.inputSchema = inputSchema;
            this.selectFunctions = selectFunctions;

            this.responseSchema = responseSchema;
            this.outputSchema = outputSchema;

            this.loggings = Logging.map(loggings);

            this.tokenTypes = new HashSet<>();
        }

        @Override
        public void setup() throws UserCodeExecutionException {

            this.client = HttpClient.newBuilder().build();

            final Parameters parameters = new Gson().fromJson(parametersText, Parameters.class);
            parameters.setDefaults();

            this.request = parameters.request;
            this.response = parameters.response;
            this.retry = parameters.retry;
            this.timeoutSecond = parameters.timeoutSecond;

            this.request.setup();

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
                final MElement element = kv.getValue();
                Logging.log(LOG, loggings, "input", element);

                final Map<String, Object> params = element.asStandardMap(inputSchema, null);
                final Map<String, Object> preparedParams = kv.getKey();
                if(preparedParams != null) {
                    params.putAll(preparedParams);
                }

                final HttpResponse<?> httpResponse = HttpUtil.sendRequest(client, request, params, HttpUtil.getBodyHandler(response.format));
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

                final Map<String, Object> outputValues = switch (this.response.format) {
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
                        yield JsonToElementConverter.convert(responseSchema.getFields(), jsonObject);
                    }
                };
                final MElement output = MElement.of(outputValues, element.getEpochMillis());
                Logging.log(LOG, loggings, "output", output);
                return output;
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

    private static class LookupDoFn extends DoFn<MElement, KV<Map<String, Object>, MElement>> {

        private final Map<String, PCollectionView<?>> views;

        LookupDoFn(final Map<String, PCollectionView<?>> views) {
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


    /*
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

     */

}
