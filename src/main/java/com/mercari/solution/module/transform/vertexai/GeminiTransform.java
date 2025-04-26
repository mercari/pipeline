package com.mercari.solution.module.transform.vertexai;

import com.google.auth.oauth2.AccessToken;
import com.google.gson.JsonObject;
import com.mercari.solution.module.*;
import com.mercari.solution.util.TemplateUtil;
import com.mercari.solution.util.coder.ElementCoder;
import com.mercari.solution.util.gcp.IAMUtil;
import com.mercari.solution.util.gcp.vertexai.GeminiUtil;
import com.mercari.solution.util.pipeline.OptionUtil;
import com.mercari.solution.util.pipeline.Union;
import freemarker.template.Template;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.net.http.HttpClient;
import java.util.*;


@Transform.Module(name="vertexai.gemini")
public class GeminiTransform extends Transform {

    private static final Logger LOG = LoggerFactory.getLogger(GeminiTransform.class);

    private static class Parameters implements Serializable {

        private Mode mode;
        private GeminiUtil.Model model; // for prediction only
        private GeminiUtil.GenerateContentRequest request;

        public void validate(Schema inputSchema) {
            final List<String> errorMessages = new ArrayList<>();

            if(mode == null) {
                errorMessages.add("parameter mode must not be null");
            } else {
                switch (mode) {
                    case predict -> {
                        if(model == null) {
                            errorMessages.add("parameter model must not be null");
                        } else {
                            errorMessages.addAll(model.validate());
                        }
                    }
                    case batch_bigquery -> {

                    }
                }
            }

            if(request == null) {
                errorMessages.add("parameter request must not be null");
            } else {
                errorMessages.addAll(request.validate());
            }

            if(!errorMessages.isEmpty()) {
                throw new IllegalModuleException(errorMessages);
            }
        }

        public void setDefaults(boolean isStreaming) {
            switch (mode) {
                case predict -> {
                    model.setDefaults();
                }
                case batch_json -> {

                }
                case batch_bigquery -> {

                }
            }
            request.setDefaults();
        }

    }

    private enum Mode {
        predict,
        batch_json,
        batch_bigquery
    }

    @Override
    public MCollectionTuple expand(
            MCollectionTuple inputs,
            MErrorHandler errorHandler) {

        final PCollection<MElement> input = inputs
                .apply("Union", Union
                        .flatten()
                        .withWaits(getWaits())
                        .withStrategy(getStrategy()));
        final Schema inputSchema = Union.createUnionSchema(inputs);

        final Parameters parameters = getParameters(Parameters.class);
        parameters.validate(inputSchema);
        parameters.setDefaults(OptionUtil.isStreaming(inputs));

        final TupleTag<MElement> formatOutputTag = new TupleTag<>(){};
        final TupleTag<MElement> formatFailureTag = new TupleTag<>(){};

        final PCollectionTuple formatted = input
                .apply("Format", ParDo
                        .of(new FormatDoFn(
                                getJobName(), getName(),
                                parameters, inputSchema, inputs.getAllInputs(),
                                formatFailureTag, getFailFast()))
                        .withOutputTags(formatOutputTag, TupleTagList.of(formatFailureTag)));

        final Schema requestSchema = GeminiUtil.GenerateContentRequest.createSchema();

        switch (parameters.mode) {
            case batch_bigquery -> {
                return MCollectionTuple
                        .of(formatted.get(formatOutputTag), Schema.builder()
                                .withField("request", Schema.FieldType.JSON)
                                .build());
            }
            case batch_json -> {
                return MCollectionTuple
                        .of(formatted.get(formatOutputTag), Schema.builder()
                                .withField("request", Schema.FieldType.element(requestSchema))
                                .build());
            }
            case predict -> {

                final TupleTag<MElement> predictOutputTag = new TupleTag<>(){};
                final TupleTag<MElement> predictFailureTag = new TupleTag<>(){};
                final PCollectionTuple predicted = formatted
                        .get(formatOutputTag)
                        .setCoder(ElementCoder.of(requestSchema))
                        .apply("Predict", ParDo
                                .of(new PredictDoFn(
                                        getJobName(), getName(),
                                        parameters, inputs.getAllInputs(),
                                        predictFailureTag, getFailFast()))
                                .withOutputTags(predictOutputTag, TupleTagList.of(predictFailureTag)));
                return MCollectionTuple
                        .of(predicted.get(predictOutputTag), requestSchema);
            }
            default -> throw new IllegalArgumentException();
        }
    }

    private static class FormatDoFn extends DoFn<MElement, MElement> {

        private final String jobName;
        private final String name;

        private final Mode mode;
        private final String requestJson;

        private final Schema inputSchema;
        private final List<String> inputNames;

        private final boolean failFast;
        private final TupleTag<MElement> failureTag;

        private final List<String> templateArgs;
        private transient Template requestTemplate;

        FormatDoFn(
                final String jobName,
                final String name,
                final Parameters parameters,
                final Schema inputSchema,
                final List<String> inputNames,
                final TupleTag<MElement> failureTag,
                final boolean failFast) {

            this.jobName = jobName;
            this.name = name;

            this.mode = parameters.mode;
            this.requestJson = parameters.request.toJson().toString();

            this.inputSchema = inputSchema;
            this.inputNames = inputNames;

            this.failFast = failFast;
            this.failureTag = failureTag;

            this.templateArgs = TemplateUtil.extractTemplateArgs(requestJson, inputSchema);
        }

        @Setup
        public void setup() {
            this.requestTemplate = TemplateUtil.createStrictTemplate("GeminiTransformFormatTemplate", requestJson);
        }

        @ProcessElement
        public void processElement(ProcessContext c) {
            final MElement element = c.element();
            if(element == null) {
                return;
            }

            try {
                final Map<String, Object> standardValues = element.asStandardMap(inputSchema, templateArgs);
                final String requestJsonString = TemplateUtil.executeStrictTemplate(requestTemplate, standardValues);
                final Map<String, Object> requestValues = GeminiUtil.GenerateContentRequest.toPrimitiveMap(requestJsonString);
                switch (mode) {
                    case batch_bigquery -> {
                        final Map<String, Object> request = Map.of("request", requestJsonString);
                        final MElement output = MElement.of(request, c.timestamp());
                        c.output(output);
                    }
                    case batch_json, predict -> {
                        final MElement output = MElement.of(requestValues, c.timestamp());
                        c.output(output);
                    }
                }
            } catch (final Throwable e) {
                final String source = inputNames.get(element.getIndex());
                final String input = element.toString();
                if(failFast) {
                    throw new RuntimeException(String.format("Failed to create message for input: %s, from: %s", input, source), e);
                }

                final MFailure failure = MFailure.of(jobName, name, input, e, c.timestamp());
                LOG.error("Failed to create message for input: {} from: {}\n cause: {}", input, source, MFailure.convertThrowableMessage(e));
                c.output(failureTag, failure.toElement(c.timestamp()));
            }
        }

    }


    private static class PredictDoFn extends DoFn<MElement, MElement> {

        private final String jobName;
        private final String name;

        private final GeminiUtil.Model model;

        private final List<String> inputNames;

        private final boolean failFast;
        private final TupleTag<MElement> failureTag;

        private transient HttpClient httpClient;
        private transient AccessToken accessToken;

        PredictDoFn(
                final String jobName,
                final String name,
                final Parameters parameters,
                final List<String> inputNames,
                final TupleTag<MElement> failureTag,
                final boolean failFast) {

            this.jobName = jobName;
            this.name = name;

            this.model = parameters.model;

            this.inputNames = inputNames;

            this.failFast = failFast;
            this.failureTag = failureTag;
        }

        @Setup
        public void setup() throws IOException {
            this.httpClient = HttpClient.newBuilder().build();
            this.accessToken = IAMUtil.getAccessToken();
        }

        @ProcessElement
        public void processElement(ProcessContext c) {
            final MElement element = c.element();
            if(element == null) {
                return;
            }

            try {
                final Date currentDate = new Date(System.currentTimeMillis() - 60000L);
                if(this.accessToken.getExpirationTime().after(currentDate)) {
                    this.accessToken = IAMUtil.getAccessToken();
                }

                final JsonObject jsonObject = GeminiUtil.GenerateContentRequest.toJson(element);
                final GeminiUtil.GenerateContentResponse response = GeminiUtil.generateContent(httpClient, accessToken.getTokenValue(), model, jsonObject);
            } catch (final Throwable e) {
                final String source = inputNames.get(element.getIndex());
                final String input = element.toString();
                if(failFast) {
                    throw new RuntimeException(String.format("Failed to create message for input: %s, from: %s", input, source), e);
                }

                final MFailure failure = MFailure.of(jobName, name, input, e, c.timestamp());
                LOG.error("Failed to create message for input: {} from: {}\n cause: {}", input, source, MFailure.convertThrowableMessage(e));
                c.output(failureTag, failure.toElement(c.timestamp()));
            }
        }

    }
}
