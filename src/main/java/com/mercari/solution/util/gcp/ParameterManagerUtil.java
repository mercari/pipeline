package com.mercari.solution.util.gcp;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.mercari.solution.util.DateTimeUtil;

import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

public class ParameterManagerUtil {

    public static final String ENDPOINT = "https://parametermanager.googleapis.com";

    private static final Pattern PATTERN_PARAMETER_NAME = Pattern
            .compile("(projects/)[a-zA-Z]+?[a-zA-Z0-9\\-]*/locations/[a-zA-Z0-9_\\-]+/parameters/[a-zA-Z0-9_\\-]+");
    private static final Pattern PATTERN_PARAMETER_VERSION_NAME = Pattern
            .compile("(projects/)[a-zA-Z]+?[a-zA-Z0-9\\-]*/locations/[a-zA-Z0-9_\\-]+/parameters/[a-zA-Z0-9_\\-]+/versions/[a-zA-Z0-9_\\-]+");


    public static class Parameter implements Serializable {

        public final String name;
        public final String format;
        public final long createTime;
        public final long updateTime;

        public static Parameter of(final JsonObject jsonObject) {
            return new Parameter(jsonObject);
        }

        public Parameter(final JsonObject jsonObject) {

            this.name = jsonObject.get("name").getAsString();
            if(jsonObject.has("format")) {
                this.format = jsonObject.get("format").getAsString();
            } else {
                this.format = null;
            }
            this.createTime = DateTimeUtil.toEpochMicroSecond(jsonObject.get("createTime").getAsString());
            this.updateTime = DateTimeUtil.toEpochMicroSecond(jsonObject.get("updateTime").getAsString());
        }

    }

    public static class Version implements Serializable {

        public final String name;
        public final byte[] payload;
        public final byte[] renderedPayload;
        public final boolean disabled;
        public final long createTime;
        public final long updateTime;

        public static Version of(final JsonObject jsonObject) {
            return new Version(null, jsonObject);
        }

        public static Version of(final String name, final JsonObject jsonObject) {
            return new Version(name, jsonObject);
        }

        public Version(final String name, final JsonObject jsonObject) {
            if(jsonObject.has("name")) {
                this.name = jsonObject.get("name").getAsString();
            } else {
                this.name = name;
            }

            if(jsonObject.has("disabled")) {
                this.disabled = jsonObject.get("disabled").getAsBoolean();
            } else {
                this.disabled = false;
            }

            if(jsonObject.has("payload")) {
                final JsonObject payloadJson = jsonObject.getAsJsonObject("payload");
                this.payload = Base64.getDecoder().decode(payloadJson.get("data").getAsString());
            } else {
                this.payload = null;
            }

            if(jsonObject.has("renderedPayload")) {
                this.renderedPayload = Base64.getDecoder().decode(jsonObject.get("renderedPayload").getAsString());
            } else {
                this.renderedPayload = null;
            }

            if(jsonObject.has("createTime")) {
                this.createTime = DateTimeUtil.toEpochMicroSecond(jsonObject.get("createTime").getAsString());
            } else {
                this.createTime = 0L;
            }

            if(jsonObject.has("updateTime")) {
                this.updateTime = DateTimeUtil.toEpochMicroSecond(jsonObject.get("updateTime").getAsString());
            } else {
                this.updateTime = 0L;
            }
        }

    }

    public static boolean isParameterResource(final String text) {
        if(text == null) {
            return false;
        }
        if(!text.startsWith("projects/")) {
            return false;
        }
        return PATTERN_PARAMETER_NAME.matcher(text).find();
    }

    public static boolean isParameterVersionResource(final String text) {
        if(text == null) {
            return false;
        }
        if(!text.startsWith("projects/")) {
            return false;
        }
        return PATTERN_PARAMETER_VERSION_NAME.matcher(text).find();
    }

    public static Parameter getParameter(final String name) {
        try(final HttpClient client = HttpClient.newHttpClient()) {
            return getParameter(client, name);
        }
    }

    public static Parameter getParameter(
            final HttpClient client,
            final String name) {

        final String endpoint = String.format("%s/%s/%s", ENDPOINT, "v1", name);
        try {
            final String accessToken = IAMUtil.getAccessToken().getTokenValue();
            final HttpRequest req = HttpRequest.newBuilder()
                    .uri(new URI(endpoint))
                    .header("Authorization", "Bearer " + accessToken)
                    .GET()
                    .build();
            final HttpResponse<String> res = client.send(req, HttpResponse.BodyHandlers.ofString());
            if(res.statusCode() >= 400) {
                throw new RuntimeException("Failed to get parameter: " + endpoint + ", status: " + res.statusCode() + ", body: " + res.body());
            }

            final String responseText = res.body();
            final JsonObject responseJson = new Gson().fromJson(responseText, JsonObject.class);
            return Parameter.of(responseJson);
        } catch (IOException | URISyntaxException | InterruptedException e) {
            throw new RuntimeException("Failed to get parameter: " + endpoint, e);
        }
    }

    public static Version getParameterVersion(
            final String parent,
            final String versionId) {

        final String name = String.format("%s/versions/%s", parent, versionId);
        return getParameterVersion(name);
    }

    public static Version getParameterVersion(final String name) {
        try(final HttpClient client = HttpClient.newHttpClient()) {
            return getParameterVersion(client, name);
        }
    }

    public static Version getParameterVersion(final HttpClient client, final String name) {
        final String endpoint = String.format("%s/%s/%s", ENDPOINT, "v1", name);
        try {
            final String accessToken = IAMUtil.getAccessToken().getTokenValue();
            final HttpRequest req = HttpRequest.newBuilder()
                    .uri(new URI(endpoint))
                    .header("Authorization", "Bearer " + accessToken)
                    .GET()
                    .build();
            final HttpResponse<String> res = client.send(req, HttpResponse.BodyHandlers.ofString());
            if(res.statusCode() >= 400) {
                throw new RuntimeException("Failed to get parameter version: " + endpoint + ", status: " + res.statusCode() + ", body: " + res.body());
            }

            final String responseText = res.body();
            final JsonObject responseJson = new Gson().fromJson(responseText, JsonObject.class);
            return Version.of(responseJson);
        } catch (IOException | URISyntaxException | InterruptedException e) {
            throw new RuntimeException("Failed to get parameter version: " + endpoint, e);
        }
    }

    public static List<Version> listParameterVersions(final String name, final String filter) {
        try(final HttpClient client = HttpClient.newHttpClient()) {
            return listParameterVersions(client, name, filter);
        }
    }

    public static List<Version> listParameterVersions(final HttpClient client, final String parent, final String filter) {
        String endpoint = String.format("%s/%s/%s", ENDPOINT, "v1", parent);
        if(filter != null) {
            endpoint = endpoint + "?filter=" + filter;
        }
        try {
            final String accessToken = IAMUtil.getAccessToken().getTokenValue();
            final HttpRequest req = HttpRequest.newBuilder()
                    .uri(new URI(endpoint))
                    .header("Authorization", "Bearer " + accessToken)
                    .GET()
                    .build();
            final HttpResponse<String> res = client.send(req, HttpResponse.BodyHandlers.ofString());
            if(res.statusCode() >= 400) {
                throw new RuntimeException("Failed to get parameter version: " + endpoint + ", status: " + res.statusCode() + ", body: " + res.body());
            }

            final String responseText = res.body();
            final JsonObject responseJson = new Gson().fromJson(responseText, JsonObject.class);
            if(!responseJson.has("parameterVersions")) {
                throw new RuntimeException("Failed to list parameter version:s " + endpoint);
            }
            List<Version> versions = new ArrayList<>();
            for(final JsonElement element : responseJson.getAsJsonArray("parameterVersions")) {
                final Version version = Version.of(element.getAsJsonObject());
                versions.add(version);
            }
            return versions;
        } catch (IOException | URISyntaxException | InterruptedException e) {
            throw new RuntimeException("Failed to get parameter version: " + endpoint, e);
        }
    }

    public static Parameter createParameter(
            final String project,
            final String location,
            final String id,
            final String format,
            final Map<String, String> labels) {

        try(final HttpClient client = HttpClient.newHttpClient()) {
            return createParameter(client, project, location, id, format, labels);
        }
    }

    public static Parameter createParameter(
            final HttpClient client,
            final String project,
            final String location,
            final String id,
            final String format,
            final Map<String, String> labels) {

        final String endpoint = String.format("%s/%s/projects/%s/locations/%s/parameters?parameterId=%s", ENDPOINT, "v1", project, location, id);
        try {
            final JsonObject body = new JsonObject();
            if(format != null) {
                body.addProperty("format", format);
            }
            if(labels != null && !labels.isEmpty()) {
                final JsonObject labelsJson = new JsonObject();
                for(final Map.Entry<String,String> entry : labels.entrySet()) {
                    labelsJson.addProperty(entry.getKey(), entry.getValue());
                }
                body.add("labels", labelsJson);
            }

            final String accessToken = IAMUtil.getAccessToken().getTokenValue();
            final HttpRequest req = HttpRequest.newBuilder()
                    .uri(new URI(endpoint))
                    .header("Authorization", "Bearer " + accessToken)
                    .POST(HttpRequest.BodyPublishers.ofString(body.toString(), StandardCharsets.UTF_8))
                    .build();
            final HttpResponse<String> res = client.send(req, HttpResponse.BodyHandlers.ofString());
            if(res.statusCode() >= 400) {
                throw new RuntimeException("Failed to get parameter: " + endpoint + ", status: " + res.statusCode() + ", body: " + res.body());
            }

            final String responseText = res.body();
            final JsonObject responseJson = new Gson().fromJson(responseText, JsonObject.class);
            return Parameter.of(responseJson);
        } catch (IOException | URISyntaxException | InterruptedException e) {
            throw new RuntimeException("Failed to get parameter: " + endpoint, e);
        }
    }

    public static Version createParameterVersion(
            final String parent,
            final String versionId,
            final byte[] payload,
            final Boolean disabled) {

        try(final HttpClient client = HttpClient.newHttpClient()) {
            return createParameterVersion(client, parent, versionId, payload, disabled);
        }
    }

    public static Version createParameterVersion(
            final HttpClient client,
            final String parent,
            final String versionId,
            final byte[] payload,
            final Boolean disabled) {

        final String endpoint = String.format("%s/%s/%s/versions?parameterVersionId=%s", ENDPOINT, "v1", parent, versionId);
        try {
            final JsonObject body = new JsonObject();
            if(disabled != null) {
                body.addProperty("disabled", disabled);
            }
            {
                final JsonObject payloadJson = new JsonObject();
                payloadJson.addProperty("data", Base64.getEncoder().encodeToString(payload));
                body.add("payload", payloadJson);
            }

            final String accessToken = IAMUtil.getAccessToken().getTokenValue();
            final HttpRequest req = HttpRequest.newBuilder()
                    .uri(new URI(endpoint))
                    .header("Authorization", "Bearer " + accessToken)
                    .POST(HttpRequest.BodyPublishers.ofString(body.toString(), StandardCharsets.UTF_8))
                    .build();
            final HttpResponse<String> res = client.send(req, HttpResponse.BodyHandlers.ofString());
            if(res.statusCode() >= 400) {
                throw new RuntimeException("Failed to create parameter version: " + endpoint + ", status: " + res.statusCode() + ", body: " + res.body());
            }

            final String responseText = res.body();
            final JsonObject responseJson = new Gson().fromJson(responseText, JsonObject.class);
            return Version.of(responseJson);
        } catch (IOException | URISyntaxException | InterruptedException e) {
            throw new RuntimeException("Failed to get parameter: " + endpoint, e);
        }
    }

    public static Version updateParameterVersion(
            final String name,
            final byte[] payload,
            final Boolean disabled) {

        try(final HttpClient client = HttpClient.newHttpClient()) {
            return updateParameterVersion(client, name, payload, disabled);
        }
    }

    public static Version updateParameterVersion(
            final HttpClient client,
            final String name,
            final byte[] payload,
            final Boolean disabled) {

        final String endpoint = String.format("%s/%s/%s", ENDPOINT, "v1", name);
        try {
            final JsonObject body = new JsonObject();
            if(disabled != null) {
                body.addProperty("disabled", disabled);
            }
            {
                final JsonObject payloadJson = new JsonObject();
                payloadJson.addProperty("data", Base64.getEncoder().encodeToString(payload));
                body.add("payload", payloadJson);
            }

            final String accessToken = IAMUtil.getAccessToken().getTokenValue();
            final HttpRequest req = HttpRequest.newBuilder()
                    .uri(new URI(endpoint))
                    .header("Authorization", "Bearer " + accessToken)
                    .method("PATCH", HttpRequest.BodyPublishers.ofString(body.toString(), StandardCharsets.UTF_8))
                    .build();
            final HttpResponse<String> res = client.send(req, HttpResponse.BodyHandlers.ofString());
            if(res.statusCode() >= 400) {
                throw new RuntimeException("Failed to update parameter version: " + endpoint + ", status: " + res.statusCode() + ", body: " + res.body());
            }

            final String responseText = res.body();
            System.out.println(responseText);
            final JsonObject responseJson = new Gson().fromJson(responseText, JsonObject.class);
            return Version.of(responseJson);
        } catch (IOException | URISyntaxException | InterruptedException e) {
            throw new RuntimeException("Failed to update parameter version: " + endpoint, e);
        }
    }

    public static Version renderParameterVersion(final String name) {
        try(final HttpClient client = HttpClient.newHttpClient()) {
            return renderParameterVersion(client, name);
        }
    }

    public static Version renderParameterVersion(
            final HttpClient client,
            final String name) {

        final String endpoint = String.format("%s/%s/%s:render", ENDPOINT, "v1", name);
        try {
            final String accessToken = IAMUtil.getAccessToken().getTokenValue();
            final HttpRequest req = HttpRequest.newBuilder()
                    .uri(new URI(endpoint))
                    .header("Authorization", "Bearer " + accessToken)
                    .GET()
                    .build();
            final HttpResponse<String> res = client.send(req, HttpResponse.BodyHandlers.ofString());
            if(res.statusCode() >= 400) {
                throw new RuntimeException("Failed to render parameter version: " + endpoint + ", status: " + res.statusCode() + ", body: " + res.body());
            }

            final String responseText = res.body();
            final JsonObject responseJson = new Gson().fromJson(responseText, JsonObject.class);
            return Version.of(name, responseJson);
        } catch (IOException | URISyntaxException | InterruptedException e) {
            throw new RuntimeException("Failed to render parameter version: " + endpoint, e);
        }
    }

    public static boolean deleteParameterVersion(final String name) {
        try(final HttpClient client = HttpClient.newHttpClient()) {
            return deleteParameterVersion(client, name);
        }
    }

    public static boolean deleteParameterVersion(
            final HttpClient client,
            final String name) {

        final String endpoint = String.format("%s/%s/%s", ENDPOINT, "v1", name);
        try {
            final String accessToken = IAMUtil.getAccessToken().getTokenValue();
            final HttpRequest req = HttpRequest.newBuilder()
                    .uri(new URI(endpoint))
                    .header("Authorization", "Bearer " + accessToken)
                    .DELETE()
                    .build();
            final HttpResponse<String> res = client.send(req, HttpResponse.BodyHandlers.ofString());
            if(res.statusCode() >= 400) {
                if(res.statusCode() == 404) {
                    return false;
                }
                throw new RuntimeException("Failed to delete parameter version: " + endpoint + ", status: " + res.statusCode() + ", body: " + res.body());
            }
            return true;
        } catch (IOException | URISyntaxException | InterruptedException e) {
            throw new RuntimeException("Failed to delete parameter version: " + endpoint, e);
        }
    }

}
