package com.mercari.solution.module.source;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.mercari.solution.module.*;
import com.mercari.solution.util.DateTimeUtil;
import com.mercari.solution.util.TemplateUtil;
import com.mercari.solution.util.coder.ElementCoder;
import com.mercari.solution.util.domain.web.HttpUtil;
import com.mercari.solution.util.pipeline.select.SelectFunction;
import com.mercari.solution.util.schema.converter.JsonToElementConverter;
import org.apache.beam.io.requestresponse.*;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
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

@Source.Module(name="http")
public class HttpSource extends Source {

    private static final Logger LOG = LoggerFactory.getLogger(HttpSource.class);

    private static class Parameters implements Serializable {

        private List<HttpUtil.Request> requests;
        private HttpUtil.Response response;

        private Long rate;
        private DateTimeUtil.TimeUnit rateUnit;
        private Integer timeoutSecond;

        private JsonElement filter;
        private JsonArray select;
        private String flattenField;

        private void validate(final String name) {

            final List<String> errorMessages = new ArrayList<>();

            if(requests == null || requests.isEmpty()) {
                errorMessages.add("");
            } else {
                for(var request : requests) {
                    errorMessages.addAll(request.validate(name));
                }
            }

            if(!errorMessages.isEmpty()) {
                throw new IllegalArgumentException(String.join(", ", errorMessages));
            }
        }

        private void setDefaults() {
            if(timeoutSecond == null) {
                timeoutSecond = 60;
            }
            for(var request : requests) {
                request.setDefaults();
            }
            if(response == null) {
                this.response = new HttpUtil.Response();
            }
            response.setDefaults();

            if(rate == null) {
                rate = 0L;
            }
        }

        public void setup() {
            for(var request : requests) {
                request.setup();
            }
            if(response != null) {
                response.setup();
            }
        }

    }


    @Override
    public MCollectionTuple expand(PBegin begin) {

        final Parameters parameters = getParameters(Parameters.class);
        parameters.validate(getName());
        parameters.setDefaults();

        final Schema responseSchema = createResponseSchema(parameters.response);
        final List<SelectFunction> selectFunctions = SelectFunction.of(parameters.select, responseSchema.getFields());
        final Schema outputSchema = createOutputSchema(responseSchema, selectFunctions, parameters.flattenField);

        final PCollection<Long> seeds;
        if(parameters.rate > 0) {
            final GenerateSequence generateSequence = GenerateSequence
                    .from(0)
                    .withRate(parameters.rate, DateTimeUtil.getDuration(parameters.rateUnit, 1L));
            seeds = begin
                    .apply("GenerateSequence", generateSequence);
        } else {
            seeds = begin
                    .apply("Seed", Create.of(0L).withCoder(VarLongCoder.of()))
                    .setCoder(VarLongCoder.of());
        }

        final HttpCaller caller = new HttpCaller(
                getName(), getParametersText(), selectFunctions, responseSchema, outputSchema);
        final RequestResponseIO<Long, MElement> requestResponseIO = RequestResponseIO
                .ofCallerAndSetupTeardown(caller, ElementCoder.of(outputSchema))
                .withTimeout(Duration.standardSeconds(parameters.timeoutSecond));

        final Result<MElement> httpResult = seeds
                .apply("WithTimestamp", ParDo.of(new TimestampDoFn()))
                .apply("HttpCall", requestResponseIO);

        final PCollection<MElement> output = httpResult.getResponses();
        final PCollection<MElement> errors = httpResult.getFailures()
                .apply("Failures", ParDo.of(new FailureDoFn(getJobName(), getName(), getFailFast())));

        return MCollectionTuple
                .of(output, outputSchema)
                .failure(errors);
    }

    private static class TimestampDoFn extends DoFn<Long, Long> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            c.output(c.timestamp().getMillis());
        }
    }

    private static class HttpCaller implements Caller<Long, MElement>, SetupTeardown {

        private final String name;
        private final String parametersText;

        private final Schema responseSchema;
        private final Schema outputSchema;

        private final List<SelectFunction> selectFunctions;

        private transient HttpClient client;
        private transient Parameters parameters;

        HttpCaller(
                final String name,
                final String parametersText,
                final List<SelectFunction> selectFunctions,
                final Schema responseSchema,
                final Schema outputSchema) {

            this.name = name;
            this.parametersText = parametersText;
            this.selectFunctions = selectFunctions;

            this.responseSchema = responseSchema;
            this.outputSchema = outputSchema;
        }

        @Override
        public void setup() throws UserCodeExecutionException {
            try {
                this.client = HttpClient.newBuilder().build();
                this.parameters = new Gson().fromJson(parametersText, Parameters.class);
                this.parameters.setDefaults();
                this.parameters.setup();

                for (final SelectFunction selectFunction : selectFunctions) {
                    selectFunction.setup();
                }

                this.responseSchema.setup();
                this.outputSchema.setup();
            } catch (Throwable e) {
                throw new UserCodeExecutionException("Failed to setup.", e);
            }
        }

        @Override
        public void teardown() {

        }

        @Override
        public MElement call(final Long epochMillis) throws UserCodeExecutionException {
            try {
                final HttpResponse.BodyHandler<?> bodyHandler = parameters.response.getBodyHandler();

                final Map<String, Object> output = new HashMap<>();
                final Map<String, Object> params = new HashMap<>();
                for(final HttpUtil.Request request : parameters.requests) {
                    final HttpResponse<?> httpResponse = HttpUtil.sendRequest(client, request, params, bodyHandler);
                    final boolean acceptable = parameters.response.acceptableStatusCodes.contains(httpResponse.statusCode());
                    if(httpResponse.statusCode() >= 400 && httpResponse.statusCode() < 500) {
                        if(!acceptable) {
                            final String errorMessage = "Illegal response code: " + httpResponse.statusCode() + ", for endpoint: " + request.endpoint + ", response: " + httpResponse.body();
                            LOG.error(errorMessage);
                            throw new UserCodeExecutionException(errorMessage);
                        } else {
                            LOG.info("Acceptable code: {}", httpResponse.statusCode());
                        }
                    }

                    final Object body = switch (parameters.response.format) {
                        case text -> httpResponse.body();
                        case bytes -> ByteBuffer.wrap((byte[])httpResponse.body());
                        case json -> {
                            final String bodyString = (String) httpResponse.body();
                            if(parameters.response.schema == null) {
                                yield bodyString;
                            } else {
                                final JsonObject jsonObject = new JsonObject();
                                final JsonElement responseJson = new Gson().fromJson(bodyString, JsonElement.class);
                                yield switch (responseSchema.getField("body").getFieldType().getType()) {
                                    case element, map, json -> {
                                        jsonObject.add("body", responseJson.getAsJsonObject());
                                        final Map<String, Object> map = JsonToElementConverter.convert(responseSchema.getFields(), jsonObject);
                                        yield map.get("body");
                                    }
                                    default -> responseJson.toString();
                                };
                            }
                        }
                    };
                    output.put("statusCode", httpResponse.statusCode());
                    output.put("headers", httpResponse.headers().map());
                    output.put("timestamp", DateTimeUtil.toEpochMicroSecond(java.time.Instant.now()));
                    output.put("body", body);

                    if(Objects.requireNonNull(body) instanceof Map map) {
                        params.putAll(map);
                    } else {
                        params.put("body", body);
                    }
                }

                if(selectFunctions.isEmpty()) {
                    return MElement.of(output, epochMillis);
                }
                final Map<String, Object> primitives = SelectFunction.apply(selectFunctions, output, Instant.now());
                return MElement.of(outputSchema, primitives, epochMillis);
            } catch (URISyntaxException e) {
                throw new UserCodeExecutionException("Illegal endpoint: " + parameters.requests, e);
            } catch (IOException | InterruptedException e) {
                throw new UserCodeRemoteSystemException("Remote error: ", e);
            } catch (Throwable e) {
                throw new UserCodeExecutionException("Failed to send http request.", e);
            }
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
                throw new RuntimeException("Failed to http source: " + output);
            }
        }

    }

    private static Schema createResponseSchema(final HttpUtil.Response response) {
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

}
