package com.mercari.solution.util.domain.web;

import com.google.gson.JsonElement;
import com.mercari.solution.module.Schema;
import com.mercari.solution.util.TemplateUtil;
import freemarker.template.Template;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class HttpUtil {

    public static class Request {

        public String endpoint;
        public String method;
        private Map<String,String> params;
        private JsonElement body;
        private Map<String, String> headers;

        private transient Template templateEndpoint;
        private transient Map<String,Template> templateParams;
        private transient Map<String,Template> templateHeaders;
        private transient Template templateBody;



        public List<String> validate(String name) {
            final List<String> errorMessages = new ArrayList<>();

            if(this.endpoint == null) {
                errorMessages.add("http transform module[" + name + "].endpoint must not be null.");
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

    }

    public enum Format {
        text,
        bytes,
        json
    }

    public static <ResponseT> HttpResponse<ResponseT> sendRequest(
            final HttpClient client,
            final Request request,
            final Map<String, Object> standardValues,
            final HttpResponse.BodyHandler<ResponseT> bodyHandler) throws IOException, InterruptedException, URISyntaxException {

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

    private static String createEndpoint(
            final Request request,
            final Map<String, Object> standardValues) {

        final String params = request.templateParams.entrySet()
                .stream()
                .map(e -> e.getKey() + "=" + URLEncoder
                        .encode(TemplateUtil
                                .executeStrictTemplate(e.getValue(), standardValues), StandardCharsets.UTF_8))
                .collect(Collectors.joining("&"));

        return TemplateUtil.executeStrictTemplate(request.templateEndpoint, standardValues) + (params.isEmpty() ? "" : ("?" + params));
    }

}
