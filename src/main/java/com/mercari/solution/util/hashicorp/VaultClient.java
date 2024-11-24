package com.mercari.solution.util.hashicorp;

import com.google.gson.*;
import com.mercari.solution.util.domain.text.JsonUtil;
import com.mercari.solution.util.gcp.IAMUtil;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;

public class VaultClient {

    private static Logger LOG = LoggerFactory.getLogger(VaultClient.class);

    private static final String HEADER_TOKEN = "X-Vault-Token";
    private static final String HEADER_NAMESPACE = "X-Vault-Namespace";

    private static final String DEFAULT_ENDPOINT_GCP_AUTH = "%s/v1/auth/gcp/login";
    private static final String ENDPOINT_VAULT_REVOKE = "/v1/auth/token/revoke-self";

    private final String vaultHost;
    private final String serviceAccount;
    private final String namespace;
    private final String role;
    private final HttpClient client;

    private final String endpointToken;

    private String token;

    public VaultClient(final String vaultHost,
                       final String serviceAccount,
                       final String namespace,
                       final String role,
                       final String endpointToken) {

        if(!vaultHost.startsWith("http://") && !vaultHost.startsWith("https://")) {
            throw new IllegalArgumentException("host must be start with http:// or https://, got: " + vaultHost);
        }

        this.vaultHost = vaultHost;
        this.serviceAccount = serviceAccount;
        this.namespace = namespace;
        this.role = role;
        this.client = HttpClient.newBuilder()
                .version(HttpClient.Version.HTTP_1_1)
                .followRedirects(HttpClient.Redirect.NEVER)
                .connectTimeout(Duration.ofSeconds(60))
                .build();

        this.endpointToken = endpointToken == null ? DEFAULT_ENDPOINT_GCP_AUTH : endpointToken;

        final String jwt = IAMUtil.signJwt(serviceAccount, 60);
        this.token = getToken(jwt);
    }

    // Calling Vault API: https://www.vaultproject.io/api/auth/gcp#login
    private String getToken(final String jwt) {

        final JsonObject jsonObject = new JsonObject();
        jsonObject.addProperty("jwt", jwt);
        jsonObject.addProperty("role", role);
        final String jwtString = jsonObject.toString();

        final String url = createUrl(vaultHost, endpointToken);

        final HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .header(HEADER_NAMESPACE, namespace)
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(jwtString))
                .build();

        try {
            final HttpResponse response = client.send(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
            final JsonObject responseJson = new Gson().fromJson(response.body().toString(), JsonObject.class);

            if (responseJson.has("errors")) {
                throw new RuntimeException(String.format("Vault token errors: %s", responseJson.toString() + " jwt: " + jwt));
            } else if (response.statusCode() != 200) {
                throw new RuntimeException(String.format("Failed to access to %s, cause: %s", url, response.body()));
            }
            return responseJson.getAsJsonObject("auth").get("client_token").getAsString();
        } catch (IOException e) {
            throw new IllegalStateException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public JsonObject call(final String endpoint,
                           final String method,
                           final String body,
                           final int retry) throws IOException, InterruptedException {

        final String url = createUrl(vaultHost, endpoint);
        final HttpRequest.BodyPublisher bodyPublisher = body == null ?
                HttpRequest.BodyPublishers.noBody() : HttpRequest.BodyPublishers.ofString(body);
        final HttpRequest.Builder request = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .header(HEADER_TOKEN, token)
                .header("Content-Type", "application/json")
                .method(method, bodyPublisher);

        if(namespace != null) {
            request.header(HEADER_NAMESPACE, namespace);
        }
        if(token != null) {
            request.header(HEADER_TOKEN, token);
        }

        final HttpResponse response = client
                .send(request.build(), HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));

        if(response.statusCode() >= 400) {
            throw new RuntimeException(String.format("Failed to call endpoint %s:%s, body: %s, status: %d, message: %s",
                    endpoint, method, body, response.statusCode(), response.body()));
        }
        final JsonObject responseJson = new Gson().fromJson(response.body().toString(), JsonObject.class);
        if(responseJson != null && responseJson.has("errors")) {
            throw new RuntimeException(String.format("Errors: endpoint %s:%s, body: %s, status: %d, message: %s",
                    endpoint, method, body, response.statusCode(), responseJson.toString()));
        }

        return responseJson;
    }

    // Calling Vault API: https://www.vaultproject.io/api-docs/auth/token#revoke-a-token-self
    public void revokeToken() throws IOException, InterruptedException {
        final JsonObject response = call(ENDPOINT_VAULT_REVOKE, "POST", "", 3);
    }

    // Calling Vault API: https://www.vaultproject.io/api/secret/kv/kv-v2.html#read-secret-version
    public JsonObject readKVSecret(final String endpoint) throws IOException, InterruptedException {
        final JsonObject responseJson = call(endpoint, "GET", null, 3);
        return responseJson.get("data").getAsJsonObject().get("data").getAsJsonObject();
    }

    // Calling Vault API: https://www.vaultproject.io/api/secret/transit/index.html#decrypt-data
    public String decryptSecret(final String endpoint,
                                final String encrypted) {

        final JsonArray batchCipherTexts = new JsonArray();
        final JsonObject childAttr = new JsonObject();
        childAttr.addProperty("ciphertext", encrypted);
        batchCipherTexts.add(childAttr);
        final JsonObject jsonObject = new JsonObject();
        jsonObject.add("batch_input", batchCipherTexts);

        try {
            final JsonObject responseJson = call(endpoint, "POST", jsonObject.toString(), 3);
            final JsonArray results = responseJson.getAsJsonObject("data").getAsJsonArray("batch_results");
            return results.get(0).getAsJsonObject().get("plaintext").getAsString();
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    // Calling Vault API: https://www.vaultproject.io/api/secret/transit/index.html#decrypt-data
    public List<String> decryptSecret(final String endpoint,
                                      final List<String> encrypted) throws IOException, InterruptedException {

        final JsonArray batchCipherTexts = new JsonArray();
        encrypted.forEach(v -> {
            final JsonObject childAttr = new JsonObject();
            childAttr.addProperty("ciphertext", v);
            batchCipherTexts.add(childAttr);
        });
        final JsonObject jsonObject = new JsonObject();
        jsonObject.add("batch_input", batchCipherTexts);

        final JsonObject responseJson = call(endpoint, "POST", jsonObject.toString(), 3);
        final JsonArray results = responseJson.getAsJsonObject("data").getAsJsonArray("batch_results");
        final List<String> decrypted = new ArrayList<>();
        for(int i=0; i<results.size(); i++) {
            decrypted.add(results.get(i).getAsJsonObject().get("plaintext").getAsString());
        }
        return decrypted;
    }

    // Calling Vault API: https://www.vaultproject.io/api/secret/transit/index.html#decrypt-data
    public JsonElement decryptSecrets(final String endpoint,
                                      final String jsonPath,
                                      final JsonElement data) {

        try {
            if (data.isJsonPrimitive()) {
                final List<String> decrypted = decryptSecret(endpoint, Arrays.asList(data.getAsString()));
                return new JsonPrimitive(decrypted.get(0));
            } else if (data.isJsonArray()) {
                final List<String> encrypted = new ArrayList<>();
                for (JsonElement element : data.getAsJsonArray()) {
                    encrypted.add(element.getAsString());
                }
                final List<String> decrypted = decryptSecret(endpoint, encrypted);
                final JsonArray array = new JsonArray(decrypted.size());
                decrypted.forEach(array::add);
                return array;
            } else {
                final List<KV<String, Object>> pathAndEncrypted = JsonUtil.read(data.getAsJsonObject(), jsonPath);

                final List<String> encrypted = pathAndEncrypted.stream()
                        .map(KV::getValue)
                        .map(Object::toString)
                        .collect(Collectors.toList());

                final List<KV<String, Object>> pathAndDecrypted = new ArrayList<>();
                final List<String> decrypted = decryptSecret(endpoint, encrypted);
                for (int i = 0; i < pathAndEncrypted.size(); i++) {
                    pathAndDecrypted.add(KV.of(pathAndEncrypted.get(i).getKey(), decrypted.get(i)));
                }
                final JsonObject result = JsonUtil.set(data.getAsJsonObject(), pathAndDecrypted);
                return result;
            }
        } catch (Exception e) {
            throw new IllegalStateException("Faild to decrypt data: " + data.toString()
                    + " with jsonPath: " + jsonPath, e);
        }
    }

    private static String createUrl(String host, String endpoint) {
        if(!host.startsWith("http://") && !host.startsWith("https://")) {
            throw new IllegalArgumentException("host must be start with http:// or https://, got: " + host);
        }
        if(host.endsWith("/")) {
           host = host.substring(0, host.length() - 1);
        }
        if(!endpoint.startsWith("/")) {
            endpoint = "/" + endpoint;
        }
        return host + endpoint;
    }

}
