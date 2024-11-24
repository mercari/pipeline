package com.mercari.solution.util.gcp.vertexai;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.mercari.solution.util.schema.OpenApiSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class GeminiUtil {

    private static final Logger LOG = LoggerFactory.getLogger(GeminiUtil.class);

    private static final String ENDPOINT = "https://%s-aiplatform.googleapis.com/v1/projects/%s/locations/%s/publishers/%s/models/%s";

    public static class Model implements Serializable {
        public String project;
        public String region;
        public String publisher;
        public String id;
    }

    public static class GenerateContentRequest implements Serializable {

        public List<Content> contents;
        public Content systemInstruction;
        public GenerationConfig generationConfig;
        public SafetySetting safetySetting;

        public JsonObject toJson() {
            final JsonObject requestJson = new JsonObject();

            if(contents != null) {
                final JsonArray contentsArray = new JsonArray();
                for(final Content content : contents) {
                    final JsonObject contentJson = content.toJson();
                    contentsArray.add(contentJson);
                }
                requestJson.add("contents", contentsArray);
            }

            if(systemInstruction != null) {
                final JsonObject systemInstructionJson = systemInstruction.toJson();
                requestJson.add("systemInstruction", systemInstructionJson);
            }

            if(generationConfig != null) {
                final JsonObject generationConfigJson = generationConfig.toJson();
                requestJson.add("generationConfig", generationConfigJson);
            }

            if(safetySetting != null) {
                final JsonObject safetySettingJson = safetySetting.toJson();
                requestJson.add("safetySetting", safetySettingJson);
            }

            return requestJson;
        }
    }

    public static class GenerateContentResponse implements Serializable {
        public List<Content> contents;
        public Content systemInstruction;
        public GenerationConfig generationConfig;
        public SafetySetting safetySetting;

        public static GenerateContentResponse of(JsonObject responseJson) {
            final GenerateContentResponse response = new GenerateContentResponse();
            return response;
        }
    }


    public static class Content implements Serializable {

        public String role;
        public List<Part> parts;

        public JsonObject toJson() {
            final JsonObject contentJson = new JsonObject();
            contentJson.addProperty("role", role);
            final JsonArray partArray = new JsonArray();
            for(final Part part : parts) {
                final JsonObject partJson = part.toJson();
                partArray.add(partJson);
            }
            contentJson.add("parts", partArray);
            return contentJson;
        }

    }

    public static class Part implements Serializable {
        public String text;
        public Blob inlineData;
        public FileData fileData;
        public FunctionCall functionCall;
        public FunctionResponse functionResponse;
        public VideoMetadata videoMetadata;

        public JsonObject toJson() {
            final JsonObject partJson = new JsonObject();
            if(text != null && !text.isEmpty()) {
                partJson.addProperty("text", text);
            } else if(inlineData != null) {
                partJson.add("inlineData", inlineData.toJson());
            } else if(fileData != null) {
                partJson.add("fileData", fileData.toJson());
            }
            return partJson;
        }
    }

    public static class GenerationConfig implements Serializable {

        public Double temperature;
        public Double topP;
        public Double topK;
        public Integer candidateCount;
        public Integer maxOutputTokens;
        public List<String> stopSequence;
        public Double presencePenalty;
        public Double frequencyPenalty;
        public String responseMimeType;
        public OpenApiSchema responseSchema;

        public JsonObject toJson() {
            final JsonObject generationConfigJson = new JsonObject();
            if(responseMimeType != null) {
                generationConfigJson.addProperty("responseMimeType", responseMimeType);
            }
            if(responseSchema != null) {
                generationConfigJson.add("responseSchema", responseSchema.toJson());
            }
            return generationConfigJson;
        }

    }

    public static class Blob {
        public String mimeType;
        public String data;

        public JsonObject toJson() {
            final JsonObject blobJson = new JsonObject();
            blobJson.addProperty("mimeType", mimeType);
            blobJson.addProperty("data", data);
            return blobJson;
        }
    }

    public static class FileData {
        public String mimeType;
        public String fileUri;

        public JsonObject toJson() {
            final JsonObject fileDataJson = new JsonObject();
            fileDataJson.addProperty("mimeType", mimeType);
            fileDataJson.addProperty("fileUri", fileUri);
            return fileDataJson;
        }
    }


    public static class FunctionCall {
        public String name;
        public String args;
    }

    public static class FunctionResponse {
        public String name;
        public String response;
    }

    public static class VideoMetadata implements Serializable {

    }

    public static class SafetySetting implements Serializable {

        public HarmCategory category;
        public HarmBlockThreshold threshold;
        public Integer maxInfluenceTerms;
        public HarmBlockMethod method;

        public JsonObject toJson() {
            final JsonObject generationConfigJson = new JsonObject();

            return generationConfigJson;
        }

    }

    public enum HarmCategory {
        HARM_CATEGORY_UNSPECIFIED,
        HARM_CATEGORY_HATE_SPEECH,
        HARM_CATEGORY_DANGEROUS_CONTENT,
        HARM_CATEGORY_HARASSMENT,
        HARM_CATEGORY_SEXUALLY_EXPLICIT
    }

    public enum HarmBlockThreshold {
        HARM_BLOCK_THRESHOLD_UNSPECIFIED,
        BLOCK_LOW_AND_ABOVE,
        BLOCK_MEDIUM_AND_ABOVE,
        BLOCK_ONLY_HIGH,
        BLOCK_NONE
    }

    public enum HarmBlockMethod {
        HARM_BLOCK_METHOD_UNSPECIFIED,
        SEVERITY,
        PROBABILITY
    }

    public static GenerateContentResponse generateContent(
            final HttpClient client,
            final String token,
            final Model model,
            final GenerateContentRequest request) {

        final String endpoint = String.format(ENDPOINT, model.region, model.project, model.region, model.publisher, model.id);
        System.out.println(endpoint);
        final JsonObject body = request.toJson();
        System.out.println(body.toString());
        try {
            final HttpRequest req = HttpRequest.newBuilder()
                    .uri(new URI(endpoint + ":generateContent"))
                    .header("Content-Type", "application/json")
                    .header("Authorization", "Bearer " + token)
                    .POST(HttpRequest.BodyPublishers.ofString(body.toString(), StandardCharsets.UTF_8))
                    .build();
            final HttpResponse<String> res = client.send(req, HttpResponse.BodyHandlers.ofString());
            //final JsonObject responseJson = new Gson().fromJson(res.body(), JsonObject.class);
            System.out.println("response: " + res.body());

            return null;
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
    }

}
