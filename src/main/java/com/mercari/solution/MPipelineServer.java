package com.mercari.solution;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.mercari.solution.config.Config;
import com.mercari.solution.module.IllegalModuleException;
import com.mercari.solution.module.MCollection;
import com.mercari.solution.module.MFailure;
import com.mercari.solution.util.schema.converter.AvroToJsonConverter;
import com.mercari.solution.util.pipeline.OptionUtil;
import com.mercari.solution.util.pipeline.Gather;
import com.mercari.solution.util.schema.AvroSchemaUtil;
import jakarta.servlet.ServletException;
import jakarta.servlet.annotation.WebServlet;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.metrics.MetricQueryResults;
import org.apache.beam.sdk.metrics.MetricResults;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.commons.compress.utils.IOUtils;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@WebServlet("/api")
public class MPipelineServer extends HttpServlet {

    @Override
    protected void doPost(
            final HttpServletRequest request,
            final HttpServletResponse response) throws IOException {

        final long startMillis = Instant.now().toEpochMilli();

        try {
            final JsonObject jsonObject = new Gson().fromJson(request.getReader(), JsonObject.class);

            if (!jsonObject.has("type")) {
                throw new IllegalArgumentException("request parameter type is not found");
            }

            if (!jsonObject.has("config")) {
                throw new IllegalArgumentException("request parameter config is not found");
            }

            final String configText = jsonObject.get("config").getAsString();

            final Config config = MPipeline.loadConfig(configText);
            final Pipeline pipeline = createPipeline();
            final Map<String, MCollection> outputs = MPipeline.apply(pipeline, config, new String[]{});

            final PipelineResult result;
            final String type = jsonObject.get("type").getAsString();
            switch (type) {
                case "run" -> {
                    result = pipeline.run();
                }
                default -> {
                    result = null;
                }
            }

            final long endMillis = Instant.now().toEpochMilli();

            final JsonObject responseJson = new JsonObject();
            responseJson.addProperty("status", "ok");
            responseJson.addProperty("millis", (endMillis - startMillis));
            {
                final JsonArray outputsArray = new JsonArray();
                for(final Map.Entry<String, MCollection> entry : outputs.entrySet()) {
                    outputsArray.add(entry.getValue().toJsonObject());
                }
                responseJson.add("outputs", outputsArray);
            }
            if(result != null) {
                responseJson.addProperty("metrics", Optional.ofNullable(result.metrics()).map(MetricResults::allMetrics).map(MetricQueryResults::toString).orElse(""));
            }
            response.getWriter().println(responseJson);
        } catch (final IllegalModuleException e) {
            final long endMillis = Instant.now().toEpochMilli();
            final JsonObject responseJson = new JsonObject();
            responseJson.addProperty("status", "error");
            responseJson.addProperty("millis", (endMillis - startMillis));
            {
                final JsonObject error = new JsonObject();
                error.addProperty("name", e.name);
                error.addProperty("module", e.module);
                final JsonArray messages = new JsonArray();
                for(final String message : e.errorMessages) {
                    messages.add(message);
                }
                error.add("messages", messages);
                responseJson.add("error", error);
            }
            response.getWriter().println(responseJson);
        } catch (final Throwable e) {
            final long endMillis = Instant.now().toEpochMilli();
            final JsonObject responseJson = new JsonObject();
            responseJson.addProperty("status", "error");
            responseJson.addProperty("millis", (endMillis - startMillis));
            {
                final JsonObject error = new JsonObject();
                error.addProperty("name", "");
                error.addProperty("module", "pipeline");
                error.addProperty("message", MFailure.convertThrowableMessage(e));
                responseJson.add("error", error);
            }
            response.getWriter().println(responseJson);
        }

    }


    @Override
    protected void doGet(
            final HttpServletRequest request,
            final HttpServletResponse response) throws ServletException, IOException {

    }

    private static Pipeline createPipeline() {
        final Pipeline pipeline = Pipeline.create(PipelineOptionsFactory
                .fromArgs(OptionUtil.filterPipelineArgs(new String[]{}))
                .as(MPipeline.MPipelineOptions.class));
        return pipeline;
    }


}
