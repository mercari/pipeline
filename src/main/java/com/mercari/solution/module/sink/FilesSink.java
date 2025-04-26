package com.mercari.solution.module.sink;

import com.mercari.solution.module.*;
import com.mercari.solution.util.TemplateUtil;
import com.mercari.solution.util.pipeline.Union;
import freemarker.template.Template;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Reshuffle;
import org.apache.beam.sdk.transforms.errorhandling.BadRecord;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;

import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

@Sink.Module(name="files")
public class FilesSink extends Sink {

    private static class Parameters implements Serializable {

        private String source;
        private String destination;
        private Map<String, String> attributes;

        private Boolean reshuffle;

        private void validate() {

        }

        private void setDefaults() {
            if(attributes == null) {
                attributes = new HashMap<>();
            }
            if(reshuffle == null) {
                reshuffle = false;
            }
        }

    }

    @Override
    public MCollectionTuple expand(
            final MCollectionTuple inputs,
            final MErrorHandler errorHandler) {

        final Parameters parameters = getParameters(Parameters.class);
        parameters.validate();
        parameters.setDefaults();

        PCollection<MElement> input = inputs
                .apply("Union", Union.flatten()
                        .withWaits(getWaits())
                        .withStrategy(getStrategy()));
        final Schema inputSchema = Union.createUnionSchema(inputs);

        if(parameters.reshuffle) {
            input = input.apply("Reshuffle", Reshuffle.viaRandomKey());
        }

        final TupleTag<MElement> outputTag = new TupleTag<>(){};
        final TupleTag<BadRecord> failureTag = new TupleTag<>(){};

        final PCollectionTuple outputs = input
                .apply("WriteFile", ParDo
                        .of(new CopyDoFn(getJobName(), getName(), inputSchema, parameters, getFailFast(), failureTag))
                        .withOutputTags(outputTag, TupleTagList.of(failureTag)));

        return MCollectionTuple
                .of(outputs.get(outputTag), createOutputSchema());
    }

    private static class CopyDoFn extends DoFn<MElement, MElement> {

        private final String jobName;
        private final String name;
        private final Schema inputSchema;

        private final String source;
        private final String destination;
        private final Map<String, String> attributes;

        private final Boolean failFast;
        private final TupleTag<BadRecord> failureTag;

        private transient Template templateSource;
        private transient Template templateDestination;
        private transient Map<String,Template> templateAttributes;

        private transient Set<String> templateArgs;

        CopyDoFn(
                final String jobName,
                final String name,
                final Schema inputSchema,
                final Parameters parameters,
                final Boolean failFast,
                final TupleTag<BadRecord> failureTag) {

            this.jobName = jobName;
            this.name = name;
            this.inputSchema = inputSchema;

            this.source = parameters.source;
            this.destination = parameters.destination;
            this.attributes = parameters.attributes;

            this.failFast = failFast;
            this.failureTag = failureTag;
        }

        @Setup
        public void setup() {

            inputSchema.setup();

            // Setup template engine
            this.templateSource = TemplateUtil.createStrictTemplate("source", source);
            this.templateDestination = TemplateUtil.createStrictTemplate("destination", destination);
            this.templateAttributes = new HashMap<>();
            for(final Map.Entry<String, String> entry : attributes.entrySet()) {
                this.templateAttributes.put(entry.getKey(), TemplateUtil.createStrictTemplate(entry.getKey(), entry.getValue()));
            }

            // Set template args
            this.templateArgs = new HashSet<>();
            this.templateArgs.addAll(TemplateUtil.extractTemplateArgs(source, inputSchema));
            this.templateArgs.addAll(TemplateUtil.extractTemplateArgs(destination, inputSchema));
            for(final Map.Entry<String, String> entry : attributes.entrySet()) {
                this.templateArgs.addAll(TemplateUtil.extractTemplateArgs(entry.getValue(), inputSchema));
            }
        }

        @ProcessElement
        public void processElement(ProcessContext c) {

        }


    }

    private static Schema createOutputSchema() {
        return Schema.builder()
                .withField("source", Schema.FieldType.STRING.withNullable(true))
                .withField("destination", Schema.FieldType.STRING.withNullable(true))
                .build();
    }

}
