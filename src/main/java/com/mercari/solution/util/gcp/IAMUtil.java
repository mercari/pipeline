package com.mercari.solution.util.gcp;

import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.gson.GsonFactory;
import com.google.api.gax.rpc.PermissionDeniedException;
import com.google.api.services.iamcredentials.v1.IAMCredentials;
import com.google.auth.Credentials;
import com.google.auth.http.HttpCredentialsAdapter;
import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.hadoop.util.ChainingHttpRequestInitializer;
import com.google.cloud.iam.credentials.v1.IamCredentialsClient;
import com.google.cloud.iam.credentials.v1.IamCredentialsSettings;
import com.google.cloud.iam.credentials.v1.SignJwtRequest;
import com.google.cloud.iam.credentials.v1.SignJwtResponse;
import com.google.common.collect.ImmutableList;
import com.google.gson.JsonObject;
import org.apache.beam.sdk.extensions.gcp.util.RetryHttpRequestInitializer;
import org.joda.time.DateTime;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

public class IAMUtil {

    private static final String ENDPOINT_METADATA = "http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/identity?audience=";

    public static String signJwt(final String serviceAccount, final int expiration) {
        final long exp = DateTime.now().plusSeconds(expiration).getMillis() / 1000;
        final JsonObject jsonObject = new JsonObject();
        jsonObject.addProperty("sub", serviceAccount);
        jsonObject.addProperty("exp", exp);
        try(final IamCredentialsClient client = IamCredentialsClient.create(IamCredentialsSettings
                .newBuilder().build())) {
            try {
                final SignJwtResponse res = client.signJwt(SignJwtRequest.newBuilder()
                        .setName(serviceAccount)
                        .setPayload(jsonObject.toString())
                        .build());
                return res.getSignedJwt();
            } catch (PermissionDeniedException e) {
                throw e;
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static String getIdToken(final HttpClient client, final String endpoint)
            throws IOException, URISyntaxException, InterruptedException {
        final String metaserver = ENDPOINT_METADATA + endpoint;
        final HttpRequest req = HttpRequest.newBuilder()
                .uri(new URI(metaserver))
                .header("Metadata-Flavor", "Google")
                .GET()
                .build();

        final HttpResponse<String> res = client.send(req, HttpResponse.BodyHandlers.ofString());
        return res.body();
    }

    public static AccessToken getAccessToken() throws IOException {
        final GoogleCredentials credentials = GoogleCredentials
                .getApplicationDefault()
                .createScoped("https://www.googleapis.com/auth/cloud-platform");
        return credentials.refreshAccessToken();
    }

    public static String signJwt(final String serviceAccount) {
        final JsonObject payload = new JsonObject();
        if(serviceAccount.startsWith("projects")) {
            var strs = serviceAccount.split("/");
            payload.addProperty("sub", strs[strs.length - 1]);
        } else {
            payload.addProperty("sub", serviceAccount);
        }
        final long exp = DateTime.now().plusSeconds(60).getMillis() / 1000;
        payload.addProperty("exp", exp);

        final HttpTransport transport = new NetHttpTransport();
        final JsonFactory jsonFactory = GsonFactory.getDefaultInstance();
        try {
            final Credentials credential = GoogleCredentials.getApplicationDefault();
            final HttpRequestInitializer initializer = new ChainingHttpRequestInitializer(
                    new HttpCredentialsAdapter(credential),
                    // Do not log 404. It clutters the output and is possibly even required by the caller.
                    new RetryHttpRequestInitializer(ImmutableList.of(404)));

            var iam = new IAMCredentials.Builder(transport, jsonFactory, initializer).build();
            var jwt = iam.projects().serviceAccounts()
                    .signJwt(serviceAccount, new com.google.api.services.iamcredentials.v1.model.SignJwtRequest()
                            .setPayload(payload.toString()))
                    .execute();
            return jwt.getSignedJwt();
        } catch (IOException e) {
            throw new RuntimeException("Failed to signJwt for service account: " + serviceAccount, e);
        }
    }

    public static String signJwt(final String serviceAccount, final JsonObject payload) {
        final HttpTransport transport = new NetHttpTransport();
        final JsonFactory jsonFactory = GsonFactory.getDefaultInstance();
        try {
            final Credentials credential = GoogleCredentials.getApplicationDefault();
            final HttpRequestInitializer initializer = new ChainingHttpRequestInitializer(
                    new HttpCredentialsAdapter(credential),
                    // Do not log 404. It clutters the output and is possibly even required by the caller.
                    new RetryHttpRequestInitializer(ImmutableList.of(404)));

            var iam = new IAMCredentials.Builder(transport, jsonFactory, initializer).build();
            var jwt = iam.projects().serviceAccounts()
                    .signJwt(serviceAccount, new com.google.api.services.iamcredentials.v1.model.SignJwtRequest()
                            .setPayload(payload.toString()))
                    .execute();
            return jwt.getSignedJwt();
        } catch (IOException e) {
            throw new RuntimeException("Failed to signJwt for service account: " + serviceAccount, e);
        }
    }


}
