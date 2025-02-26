package com.mercari.solution.util.gcp;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.regex.Pattern;


public class ArtifactRegistryUtil {

    public static final String ENDPOINT = "https://artifactregistry.googleapis.com";

    private static final Pattern PATTERN_FILE_NAME = Pattern
            .compile("(projects/)[a-zA-Z]+?[a-zA-Z0-9\\-]*/locations/[a-zA-Z0-9_\\-]+/repositories/[a-zA-Z0-9_\\-]+/files/[a-zA-Z0-9_\\-]+");

    public static boolean isArtifactRegistryResource(final String text) {
        if(text == null) {
            return false;
        }
        if(text.startsWith("ar://")) {
            return true;
        }
        if(!text.startsWith("projects/")) {
            return false;
        }
        return PATTERN_FILE_NAME.matcher(text).find();
    }

    public static byte[] download(String resource) {
        if(resource == null) {
            throw new IllegalArgumentException("Artifact resource must not be null");
        }
        if(resource.startsWith("ar://")) {
            resource = convertToResource(resource);
        }
        final String endpoint = String.format("%s/%s/%s:download?alt=media", ENDPOINT, "v1", resource);
        try(final HttpClient client = HttpClient.newHttpClient()) {
            final String accessToken = IAMUtil.getAccessToken().getTokenValue();
            final HttpRequest req = HttpRequest.newBuilder()
                    .uri(new URI(endpoint))
                    .header("Authorization", "Bearer " + accessToken)
                    .GET()
                    .build();
            final HttpResponse<byte[]> res = client.send(req, HttpResponse.BodyHandlers.ofByteArray());
            if(res.statusCode() >= 400) {
                throw new RuntimeException("Failed to download artifact: " + resource + ", status: " + res.statusCode() + ", body: " + new String(res.body(), StandardCharsets.UTF_8));
            }
            return res.body();
        } catch (IOException | URISyntaxException | InterruptedException e) {
            throw new RuntimeException("Failed to download artifact file: " + resource, e);
        }
    }

    public static String convertToResource(final String uri) {
        if(uri == null || !uri.startsWith("ar://")) {
            throw new IllegalArgumentException("Illegal artifact uri: " + uri);
        }
        final String[] paths = uri
                .replaceFirst("ar://", "")
                .split("/", 4);

        if(paths.length < 4) {
            throw new IllegalArgumentException("Illegal artifact uri: " + uri);
        }

        final String project = paths[0];
        final String region = paths[1];
        final String repository = paths[2];
        final String file = paths[3];

        return String.format("projects/%s/locations/%s/repositories/%s/files/%s", project, region, repository, file);
    }

}
