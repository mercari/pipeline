package com.mercari.solution.api;

import com.google.dataflow.v1beta3.Job;
import com.google.dataflow.v1beta3.LaunchFlexTemplateParameter;
import com.google.dataflow.v1beta3.LaunchFlexTemplateResponse;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.mercari.solution.MPipeline;
import com.mercari.solution.config.Config;
import com.mercari.solution.config.Options;
import com.mercari.solution.config.options.DataflowOptions;
import com.mercari.solution.config.options.DirectOptions;
import com.mercari.solution.module.IllegalModuleException;
import com.mercari.solution.module.MCollection;
import com.mercari.solution.module.MFailure;
import com.mercari.solution.util.gcp.DataflowUtil;
import com.mercari.solution.util.pipeline.OptionUtil;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.metrics.MetricQueryResults;
import org.apache.beam.sdk.metrics.MetricResults;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.joda.time.Duration;

import java.io.IOException;
import java.io.Reader;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class PipelineService {

    public static void serve(
            final HttpServletRequest request,
            final HttpServletResponse response) throws ServletException, IOException {

        String type = "";
        final long startMillis = Instant.now().toEpochMilli();
        try(final Reader reader = request.getReader()) {
            final JsonObject jsonObject = Config.convertConfigJson(reader);

            if (!jsonObject.has("type")) {
                throw new IllegalArgumentException("request parameter type is not found");
            }
            type = jsonObject.get("type").getAsString();

            if (!jsonObject.has("config")) {
                throw new IllegalArgumentException("request parameter config is not found");
            }
            final String configText = jsonObject.get("config").getAsString();

            switch (type.toLowerCase()) {
                case "run", "dryrun" -> {
                    final boolean dryRun = type.startsWith("dry");
                    run(configText, dryRun, response);
                }
                case "launch" -> {
                    launch(configText, response);
                }
                default -> throw new IllegalArgumentException("Not supported type: " + type);
            }
        } catch (final Throwable e) {
            final long endMillis = Instant.now().toEpochMilli();
            final JsonObject responseJson = new JsonObject();
            responseJson.addProperty("type", type);
            responseJson.addProperty("status", "error");
            responseJson.addProperty("millis", (endMillis - startMillis));
            {
                final JsonObject error = new JsonObject();
                error.addProperty("name", "");
                error.addProperty("module", "server");
                error.addProperty("message", MFailure.convertThrowableMessage(e));
                responseJson.add("error", error);
            }
            response.getWriter().println(responseJson);
        }

    }

    private static void launch(
            final String configText,
            final HttpServletResponse response) throws IOException {

        final JsonObject responseJson = new JsonObject();
        responseJson.addProperty("type", "launch");

        final long startMillis = Instant.now().toEpochMilli();
        try {
            final Config config = MPipeline.loadConfig(configText);
            if(config.getOptions() == null) {
                throw new IllegalArgumentException("config.options must not be empty");
            }
            if(config.getOptions().getDataflow() == null) {
                throw new IllegalArgumentException("config.options.dataflow must not be empty");
            }
            if(config.getOptions().getDataflow().getProject() == null) {
                throw new IllegalArgumentException("config.options.dataflow.project must not be empty");
            }
            if(config.getOptions().getDataflow().getRegion() == null) {
                throw new IllegalArgumentException("config.options.dataflow.region must not be empty");
            }
            if(config.getOptions().getDataflow().getTemplateLocation() == null) {
                throw new IllegalArgumentException("config.options.dataflow.templateLocation must not be empty");
            } else if(!config.getOptions().getDataflow().getTemplateLocation().startsWith("gs://")) {
                throw new IllegalArgumentException("config.options.dataflow.templateLocation must be starts with gs://");
            }

            final String project = config.getOptions().getDataflow().getProject();
            final String region = config.getOptions().getDataflow().getRegion();
            final String template = config.getOptions().getDataflow().getTemplateLocation();
            final Map<String, String> parameter = new HashMap<>();
            parameter.put("config", configText);
            final LaunchFlexTemplateParameter f = DataflowOptions
                    .createLaunchFlexTemplateParameter(template, parameter, config.getOptions());
            final LaunchFlexTemplateResponse resp = DataflowUtil
                    .launchFlexTemplate(project, region, f, false);

            final long endMillis = Instant.now().toEpochMilli();
            responseJson.addProperty("millis", (endMillis - startMillis));
            if(!resp.hasJob()) {
                responseJson.addProperty("status", "error");
                responseJson.addProperty("module", "pipeline");
                {
                    final JsonObject error = new JsonObject();
                    error.addProperty("name", "");
                    error.addProperty("message", "Job not found: " + resp.getJob());
                    responseJson.add("error", error);
                }
            } else {
                responseJson.addProperty("status", "ok");
                final Job job = resp.getJob();
                {
                    final JsonObject jobObject = new JsonObject();
                    jobObject.addProperty("id", job.getId());
                    jobObject.addProperty("name", job.getName());
                    jobObject.addProperty("project", job.getProjectId());
                    jobObject.addProperty("location", job.getLocation());
                    jobObject.addProperty("createTime", Instant.ofEpochSecond(job.getCreateTime().getSeconds(), job.getCreateTime().getNanos()).toString());
                    responseJson.add("job", jobObject);
                }
            }
            response.getWriter().println(responseJson);
        } catch (final Throwable e) {
            final long endMillis = Instant.now().toEpochMilli();
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

    private static void run(
            final String configText,
            final Boolean dryRun,
            final HttpServletResponse response) throws IOException {

        final long startMillis = Instant.now().toEpochMilli();
        try {

            //response.setBufferSize(0);
            //response.setHeader("Transfer-Encoding", "chunked");
            //response.getWriter().println("okokokokokok");
            //response.getWriter().flush();
            //response.flushBuffer();

            final Config config = MPipeline.loadConfig(configText);

            final MPipeline.MPipelineOptions pipelineOptions = createPipelineOptions(new String[0]);
            Options.setOptions(pipelineOptions, config.getOptions());
            if(OptionUtil.isStreaming(pipelineOptions)) {
                if(DirectOptions.isBlockOnRun(pipelineOptions)) {
                    DirectOptions.setBlockOnRun(pipelineOptions, false);
                }
            }

            final Pipeline pipeline = Pipeline.create(pipelineOptions);

            final Map<String, MCollection> outputs = MPipeline.apply(pipeline, config, new String[0]);

            final PipelineResult result = dryRun ? null : pipeline.run();

            final long endMillis = Instant.now().toEpochMilli();

            final JsonObject responseJson = new JsonObject();
            responseJson.addProperty("status", "ok");
            responseJson.addProperty("millis", (endMillis - startMillis));

            //final String jobName = pipeline.getOptions().getJobName();
            //final Map<String, List<String>> map = serverLogHandler.getMessages(Level.ALL);
            {
                final JsonArray outputsArray = new JsonArray();
                for(final Map.Entry<String, MCollection> entry : outputs.entrySet()) {
                    outputsArray.add(entry.getValue().toJsonObject());
                }
                responseJson.add("outputs", outputsArray);
            }
            if(result != null) {
                System.out.println("result not null");
                if(OptionUtil.isStreaming(pipelineOptions)) {
                    final PipelineResult.State state = result.waitUntilFinish(Duration.standardSeconds(10));
                    if(state == null || !state.isTerminal()) {
                        result.cancel();
                    }
                }
                final String metricsJson = Optional
                        .of(result.metrics())
                        .map(MetricResults::allMetrics)
                        .map(MetricQueryResults::toString)
                        .orElse("");
                responseJson.addProperty("metrics", metricsJson);
            }

            /*
            final JsonObject j = new JsonObject();
            for(Map.Entry<String, List<String>> entry : map.entrySet()) {
                JsonArray jsonArray = new JsonArray();
                for(String s : entry.getValue()) {
                    jsonArray.add(s);
                }
                j.add(entry.getKey(), jsonArray);
            }
            responseJson.add("j", j);
             */

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

    private static MPipeline.MPipelineOptions createPipelineOptions(final String[] args) {

        final String defaultProjectId = OptionUtil.getDefaultProject();
        if(defaultProjectId != null) {
            String[] strs = new String[1];
            strs[0] = defaultProjectId;
            return PipelineOptionsFactory
                    .fromArgs(OptionUtil.filterPipelineArgs(args))
                    .as(MPipeline.MPipelineOptions.class);
        } else {
            return PipelineOptionsFactory
                    .fromArgs(OptionUtil.filterPipelineArgs(args))
                    .as(MPipeline.MPipelineOptions.class);
        }

    }

}
