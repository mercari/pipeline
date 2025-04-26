package com.mercari.solution.util.domain.web;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.mercari.solution.module.Schema;
import com.mercari.solution.util.TemplateUtil;
import com.mercari.solution.util.gcp.IAMUtil;
import freemarker.template.Template;
import org.joda.time.DateTime;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Collectors;

public class HttpUtil {

    public static class Request {

        public String endpoint;
        public String method;
        private Map<String,String> params;
        private JsonElement body;
        private Map<String, String> headers;
        private Jwt jwt;
        private Type type;

        private transient Template templateEndpoint;
        private transient Map<String,Template> templateParams;
        private transient Map<String,Template> templateHeaders;
        private transient Template templateBody;

        public List<String> validate(String name) {
            final List<String> errorMessages = new ArrayList<>();

            if(this.endpoint == null) {
                errorMessages.add("http transform module[" + name + "].endpoint must not be null.");
            }

            if(this.jwt != null) {
                errorMessages.addAll(this.jwt.validate(name));
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
            if(this.type == null) {
                this.type = Type.custom;
            }
            if(this.jwt != null) {
                this.jwt.setDefaults();
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
        }

        public JsonObject toJson() {
            final JsonObject jsonObject = new JsonObject();
            jsonObject.addProperty("endpoint", endpoint);
            jsonObject.addProperty("method", method);
            if(params != null){
                final JsonObject paramsJsonObject = new JsonObject();
                for(final Map.Entry<String, String> entry : params.entrySet()) {
                    paramsJsonObject.addProperty(entry.getKey(), entry.getValue());
                }
                jsonObject.add("params", paramsJsonObject);
            }
            if(headers != null){
                final JsonObject headersJsonObject = new JsonObject();
                for(final Map.Entry<String, String> entry : params.entrySet()) {
                    headersJsonObject.addProperty(entry.getKey(), entry.getValue());
                }
                jsonObject.add("headers", headersJsonObject);
            }
            if(body != null) {
                jsonObject.add("body", body);
            }

            return jsonObject;
        }

        public String toJsonString() {
            final JsonObject jsonObject = toJson();
            return jsonObject.toString();
        }

        public static Request fromJsonString(final String jsonString) {
            return new Gson().fromJson(jsonString, Request.class);
        }
    }

    public static class Response {

        public Format format;
        public JsonElement schema;
        public List<Integer> acceptableStatusCodes;

        private transient Schema schema_;

        public HttpResponse.BodyHandler<?> getBodyHandler() {
            return switch (format) {
                case text, json -> HttpResponse.BodyHandlers.ofString();
                case bytes -> HttpResponse.BodyHandlers.ofByteArray();
            };
        }

        public List<String> validate(String name) {
            final List<String> errorMessages = new ArrayList<>();
            if(this.format == null) {
                errorMessages.add("http transform module[" + name + "].format must not be null.");
            }
            if(this.schema == null || !this.schema.isJsonObject()) {
                errorMessages.add("http transform module[" + name + "].schema must not be empty.");
            }
            if(this.acceptableStatusCodes != null) {
                for(final Integer acceptableStatusCode : acceptableStatusCodes) {
                    if(acceptableStatusCode == null) {
                        errorMessages.add("http transform module[" + name + "].acceptableStatusCodes value must not be null.");
                    } else if(acceptableStatusCode >= 600 || acceptableStatusCode < 100) {
                        errorMessages.add("http transform module[" + name + "].acceptableStatusCodes value[" + acceptableStatusCode + "] must be between 100 and 599");
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

        public void setup() {
            if(schema != null && schema.isJsonObject()) {
                this.schema_ = Schema.parse(schema);
            }
        }

        public String toJsonString() {
            final JsonObject jsonObject = new JsonObject();
            jsonObject.addProperty("format", format.name());
            if(acceptableStatusCodes != null){
                final JsonArray acceptableStatusCodesArray = new JsonArray();
                for(final Integer acceptableStatusCode : acceptableStatusCodes) {
                    acceptableStatusCodesArray.add(acceptableStatusCode);
                }
                jsonObject.add("acceptableStatusCodes", acceptableStatusCodesArray);
            }
            if(schema != null){
                jsonObject.add("schema", schema);
            }

            return jsonObject.toString();
        }

        public static Response fromJsonString(final String jsonString) {
            return new Gson().fromJson(jsonString, Response.class);
        }

    }

    public static class Jwt {
        public String name;
        public String account;
        public Integer expireMinutes;
        public Map<String, Object> payload;

        public List<String> validate(String name) {
            final List<String> errorMessages = new ArrayList<>();
            return errorMessages;
        }

        public void setDefaults() {

        }
    }

    public enum Format {
        text,
        bytes,
        json
    }

    public enum Type {
        custom,
        oauth,
        openid
    }

    public static HttpResponse.BodyHandler<?> getBodyHandler(final Format format) {
        return switch (format) {
            case text, json -> HttpResponse.BodyHandlers.ofString();
            case bytes -> HttpResponse.BodyHandlers.ofByteArray();
        };
    }

    public static Schema createResponseSchema(final Response response) {
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
                .withField("timestamp", Schema.FieldType.TIMESTAMP)
                .build();
    }

    public static <ResponseT> HttpResponse<ResponseT> sendRequest(
            final HttpClient client,
            final Request request,
            final Map<String, Object> standardValues,
            final HttpResponse.BodyHandler<ResponseT> bodyHandler) throws IOException, InterruptedException, URISyntaxException {

        if(request.jwt != null) {
            final Object code = getJwt(request.endpoint, request.jwt);
            standardValues.put(request.jwt.name, code);
        }

        final String url = createEndpoint(request, standardValues);
        final String bodyText;
        if(request.templateBody != null) {
            bodyText = TemplateUtil.executeStrictTemplate(request.templateBody, standardValues);
        } else {
            bodyText = "";
        }
        final HttpRequest.BodyPublisher bodyPublisher = HttpRequest.BodyPublishers.ofString(bodyText);

        HttpRequest.Builder builder = HttpRequest.newBuilder()
                .uri(new URI(url))
                .timeout(java.time.Duration.ofSeconds(100))
                .method(request.method.toUpperCase(), bodyPublisher);

        for(final Map.Entry<String, Template> entry : request.templateHeaders.entrySet()) {
            final String headerValue = TemplateUtil.executeStrictTemplate(entry.getValue(), standardValues);
            builder = builder.header(entry.getKey(), headerValue);
        }

        return client.send(builder.build(), bodyHandler);
    }

    private static String getJwt(final String endpoint, final Jwt jwt) {

        final JsonObject payload = new JsonObject();
        for(Map.Entry<String, Object> entry : jwt.payload.entrySet()) {
            switch (entry.getValue()) {
                case String s -> payload.addProperty(entry.getKey(), s);
                case Number i -> payload.addProperty(entry.getKey(), i);
                case Boolean b -> payload.addProperty(entry.getKey(), b);
                default -> {}
            }
        }
        if(!payload.has("aud")) {
            payload.addProperty("aud", endpoint);
        }
        if(!payload.has("jti")) {
            payload.addProperty("jti", UUID.randomUUID().toString());
        }
        if(!payload.has("exp")) {
            final long exp = DateTime.now().plusMinutes(jwt.expireMinutes).getMillis() / 1000;
            payload.addProperty("exp", exp);
        }
        return IAMUtil.signJwt(jwt.account, payload);
    }

    private static String createEndpoint(
            final Request request,
            final Map<?, ?> standardValues) {

        final String params = request.templateParams.entrySet()
                .stream()
                .map(e -> e.getKey() + "=" + URLEncoder
                        .encode(TemplateUtil
                                .executeStrictTemplate(e.getValue(), standardValues), StandardCharsets.UTF_8))
                .collect(Collectors.joining("&"));

        return TemplateUtil.executeStrictTemplate(request.templateEndpoint, standardValues) + (params.isEmpty() ? "" : ("?" + params));
    }

}
