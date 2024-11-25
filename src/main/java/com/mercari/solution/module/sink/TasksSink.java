package com.mercari.solution.module.sink;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.mercari.solution.module.*;
import com.mercari.solution.util.coder.ElementCoder;
import com.mercari.solution.util.pipeline.Union;
import org.apache.beam.io.requestresponse.Caller;
import org.apache.beam.io.requestresponse.RequestResponseIO;
import org.apache.beam.io.requestresponse.SetupTeardown;
import org.apache.beam.io.requestresponse.UserCodeExecutionException;
import org.apache.beam.sdk.values.PCollection;
import org.apache.commons.lang.NotImplementedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.*;

@Sink.Module(name="tasks")
public class TasksSink extends Sink {

    private static final Logger LOG = LoggerFactory.getLogger(TasksSink.class);

    private static class TasksSinkParameters implements Serializable {

        private String queue;
        private Format format;
        private List<String> attributes;

        private Integer maxBatchSize;
        private Integer maxBatchBytesSize;


        private void validate() {
            final List<String> errorMessages = new ArrayList<>();
            if (this.queue == null) {
                errorMessages.add("parameters.queue must not be null");
            }
            if (this.format == null) {
                errorMessages.add("parameters.format must not be null");
            }
            if (!errorMessages.isEmpty()) {
                throw new IllegalModuleException(errorMessages);
            }
        }

        private void setDefaults() {
            if (this.attributes == null) {
                this.attributes = new ArrayList<>();
            }
        }
    }

    private enum Format {
        avro,
        json,
        protobuf
    }

    @Override
    public MCollectionTuple expand(MCollectionTuple inputs) {

        if(true) {
            throw new NotImplementedException("Not Implemented tasks sink module");
        }

        final TasksSinkParameters parameters = getParameters(TasksSinkParameters.class);
        if (parameters == null) {
            throw new IllegalArgumentException("tasks sink module parameters must not be empty!");
        }
        parameters.validate();
        parameters.setDefaults();

        final PCollection<MElement> input = inputs
                .apply("Union", com.mercari.solution.util.pipeline.Union.flatten()
                        .withWaits(getWaits())
                        .withStrategy(getStrategy()));
        final Schema inputSchema = Union.createUnionSchema(inputs);

        final Caller<MElement, MElement> caller = new TasksCaller();
        RequestResponseIO.of(caller, ElementCoder.of(inputSchema));

        return null;
    }

    private static class TasksCaller implements Caller<MElement, MElement>, SetupTeardown {

        private static final String METADATA_ENDPOINT = "http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/token";
        private static final String TASKS_TASK_ENDPOINT = "https://cloudtasks.googleapis.com/v2/parent=projects/%s/locations/%s/queues/%s/tasks/%s";

        private transient HttpClient client;
        private transient String accessToken;

        @Override
        public void setup() throws UserCodeExecutionException {

            this.client = HttpClient.newBuilder().build();
            this.accessToken = getAccessToken();
        }

        @Override
        public void teardown() throws UserCodeExecutionException {

        }

        @Override
        public MElement call(MElement request) throws UserCodeExecutionException {
            try {
                final String endpoint = String.format(TASKS_TASK_ENDPOINT, "");
                final HttpRequest httpRequest = HttpRequest.newBuilder()
                        .uri(new URI(endpoint))
                        .header("Authorization", "Bearer " + accessToken)
                        .POST(HttpRequest.BodyPublishers.ofString(""))
                        .build();
                final HttpResponse<String> httpResponse = this.client
                        .send(httpRequest, HttpResponse.BodyHandlers.ofString());
                final JsonElement responseJson = new Gson().fromJson(httpResponse.body(), JsonElement.class);
                if(!responseJson.isJsonObject()) {
                    throw new IllegalStateException("Illegal token response: " + responseJson);
                }
                final JsonObject jsonObject = responseJson.getAsJsonObject();

            } catch (URISyntaxException | IOException | InterruptedException e) {
                throw new RuntimeException(e);
            }
            return null;
        }

        private JsonObject createRequest() {
            final JsonObject request = new JsonObject();
            {
                final JsonObject task = new JsonObject();
                task.addProperty("name", "");
                task.addProperty("scheduleTime", "");
                task.addProperty("dispatchDeadline", "");
                {
                    final JsonObject httpRequest = new JsonObject();
                    httpRequest.addProperty("url", "");
                    httpRequest.addProperty("httpMethod", "");
                    httpRequest.addProperty("headers", "");
                    httpRequest.addProperty("body", "");
                    //httpRequest.addProperty("oauthToken", "");
                    //httpRequest.addProperty("oidcToken", "");
                    task.add("httpRequest", httpRequest);
                }
                request.add("task", task);
            }
            return request;
        }

        private String getAccessToken() {
            try {
                final HttpRequest httpRequest = HttpRequest.newBuilder()
                        .uri(new URI(METADATA_ENDPOINT))
                        .header("Metadata-Flavor", "Google")
                        .GET()
                        .build();
                final HttpResponse<String> httpResponse = this.client
                        .send(httpRequest, HttpResponse.BodyHandlers.ofString());
                final JsonElement responseJson = new Gson().fromJson(httpResponse.body(), JsonElement.class);
                if(!responseJson.isJsonObject()) {
                    throw new IllegalStateException("Illegal token response: " + responseJson);
                }
                final JsonObject jsonObject = responseJson.getAsJsonObject();
                if(!jsonObject.has("")) {
                    throw new IllegalStateException("Illegal token response: " + responseJson);
                }

                return jsonObject.get("").getAsString();
            } catch (URISyntaxException | IOException | InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }


}