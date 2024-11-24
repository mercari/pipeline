package com.mercari.solution.util.pipeline.select;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.mercari.solution.module.Schema;
import com.mercari.solution.util.TemplateUtil;
import freemarker.template.Template;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class Http implements SelectFunction {

    private static final Logger LOG = LoggerFactory.getLogger(Http.class);

    private final String name;
    private final String endpoint;
    private final String method;
    private final String body;
    private final Map<String, String> headers;

    private final Set<String> templateArgs;
    private final List<Schema.Field> inputFields;
    private final Schema.FieldType outputFieldType;
    private final boolean ignore;


    private transient HttpClient httpClient;
    private transient Template endpointTemplate;
    private transient Template bodyTemplate;

    Http(String name,
         String endpoint,
         String method,
         String body,
         Map<String, String> headers,
         List<Schema.Field> inputFields,
         Schema.FieldType outputFieldType,
         Set<String> templateArgs,
         boolean ignore) {

        this.name = name;
        this.endpoint = endpoint;
        this.method = method;
        this.body = body;
        this.headers = headers;

        this.templateArgs = templateArgs;

        this.inputFields = inputFields;
        this.outputFieldType = outputFieldType;
        this.ignore = ignore;
    }

    public static Http of(String name, JsonObject jsonObject, List<Schema.Field> inputFields, boolean ignore) {
        if(!jsonObject.has("endpoint")) {
            throw new IllegalArgumentException("SelectField http: " + name + " requires endpoint parameter");
        }
        final String endpoint = jsonObject.get("endpoint").getAsString();

        final String method;
        if(jsonObject.has("method")) {
            method = jsonObject.get("method").getAsString();
        } else {
            method = "get";
        }

        final String body;
        if(jsonObject.has("body")) {
            body = jsonObject.get("body").getAsString();
        } else {
            body = "";
        }

        final Map<String, String> headers = new HashMap<>();
        if(jsonObject.has("headers")) {
            for(final Map.Entry<String, JsonElement> entry : jsonObject.get("headers").getAsJsonObject().entrySet()) {
                headers.put(entry.getKey(), entry.getValue().toString());
            }
        }

        final Schema.FieldType outputFieldType;
        if(jsonObject.has("type")) {
            final String type = jsonObject.get("type").getAsString();
            outputFieldType = switch (type) {
                case "string" -> Schema.FieldType.STRING.withNullable(true);
                case "bytes" -> Schema.FieldType.BYTES.withNullable(true);
                default -> throw new IllegalArgumentException();
            };
        } else {
            outputFieldType = Schema.FieldType.STRING.withNullable(true);
        }

        final Set<String> templateArgs = new HashSet<>();
        final List<String> endpointTemplateArgs = TemplateUtil.extractTemplateArgs(endpoint, inputFields);
        final List<String> bodyTemplateArgs = TemplateUtil.extractTemplateArgs(body, inputFields);
        templateArgs.addAll(endpointTemplateArgs);
        templateArgs.addAll(bodyTemplateArgs);

        return new Http(name, endpoint, method, body, headers, inputFields, outputFieldType, templateArgs, ignore);
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public boolean ignore() {
        return ignore;
    }

    @Override
    public List<Schema.Field> getInputFields() {
        return inputFields;
    }

    @Override
    public Schema.FieldType getOutputFieldType() {
        return outputFieldType;
    }

    @Override
    public void setup() {
        this.httpClient = HttpClient.newBuilder()
                .build();
        this.endpointTemplate = TemplateUtil.createStrictTemplate("HttpSelectEndpoint", endpoint);
        this.bodyTemplate = TemplateUtil.createStrictTemplate("HttpSelectBody", body);
    }

    @Override
    public Object apply(Map<String, Object> input, Instant timestamp) {
        final Map<String, Object> values = copy(input, templateArgs);
        TemplateUtil.setFunctions(values);
        final String url;
        try {
            url = TemplateUtil.executeStrictTemplate(endpointTemplate, values);
        } catch (Exception e) {
            return null;
        }

        final String body = TemplateUtil.executeStrictTemplate(bodyTemplate, values);
        final HttpRequest.BodyPublisher bodyPublisher = HttpRequest.BodyPublishers.ofString(body);

        final HttpRequest httpRequest = createHttpRequest(url, method, headers, bodyPublisher);
        final HttpResponse.BodyHandler<?> bodyHandler = switch (outputFieldType.getType()) {
            case string -> HttpResponse.BodyHandlers.ofString();
            case bytes -> HttpResponse.BodyHandlers.ofByteArray();
            default -> throw new IllegalArgumentException();
        };
        try {
            final HttpResponse<?> httpResponse = httpClient.send(httpRequest, bodyHandler);
            if(httpResponse.statusCode() == 404 || httpResponse.statusCode() == 400) {
                LOG.warn("http 4XX error for endpoint: {}, statusCode: {}, body: {}",
                        url, httpResponse.statusCode(), httpResponse.body());
                return null;
            }
            if(httpResponse.statusCode() > 400 && httpResponse.statusCode() < 500) {
                throw new IllegalArgumentException("http error for endpoint: " + url + ", statusCode: " + httpResponse.statusCode() + ", body: " + httpResponse.body());
            }
            final Object output = httpResponse.body();
            return switch (outputFieldType.getType()) {
                case string -> switch (output) {
                    case String s -> s;
                    case byte[] b -> new String(b, StandardCharsets.UTF_8);
                    case Object o -> o.toString();
                };
                case bytes -> switch (output) {
                    case String s -> ByteBuffer.wrap(Base64.getDecoder().decode(s));
                    case byte[] b -> ByteBuffer.wrap(b);
                    default -> throw new IllegalArgumentException();
                };
                default -> throw new IllegalArgumentException();
            };
        } catch (IOException | InterruptedException e) {
            throw new IllegalArgumentException();
        }
    }

    private static HttpRequest createHttpRequest(final String url, final String method, final Map<String, String> headers, final HttpRequest.BodyPublisher bodyPublisher) {
        final URI uri;
        try {
            uri = new URI(url);
        } catch (final URISyntaxException e) {
            throw new IllegalArgumentException(e);
        }
        HttpRequest.Builder builder = HttpRequest.newBuilder().uri(uri);
        for(Map.Entry<String, String> header : headers.entrySet()) {
            builder = builder.header(header.getKey(), header.getValue());
        }

        builder = switch (method.toLowerCase()) {
            case "get" -> builder.GET();
            case "post" -> builder.POST(bodyPublisher);
            case "put" -> builder.PUT(bodyPublisher);
            case "delete" -> builder.DELETE();
            default -> throw new IllegalArgumentException("Not supported method: " + method);
        };

        return builder.build();
    }

    private static Map<String, Object> copy(final Map<String, Object> values, final Set<String> fields) {
        final Map<String, Object> newValues = new HashMap<>();
        if(values == null) {
            return newValues;
        }
        for(final Map.Entry<String, Object> entry : values.entrySet()) {
            if(fields.contains(entry.getKey())) {
                newValues.put(entry.getKey(), entry.getValue());
            }
        }
        return newValues;
    }

}
