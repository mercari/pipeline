package com.mercari.solution.module.sink;

import com.mercari.solution.module.MCollectionTuple;
import com.mercari.solution.module.MElement;
import com.mercari.solution.module.Schema;
import com.mercari.solution.module.Sink;
import com.mercari.solution.util.TemplateUtil;
import com.mercari.solution.util.pipeline.Union;
import freemarker.template.Template;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Reshuffle;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

@Sink.Module(name="files")
public class FilesSink extends Sink {

    private static final Logger LOG = LoggerFactory.getLogger(FilesSink.class);

    private static final Counter ERROR_COUNTER = Metrics.counter("files", "error");

    private static class FilesSinkParameters implements Serializable {

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
    public MCollectionTuple expand(MCollectionTuple inputs) {

        final FilesSinkParameters parameters = getParameters(FilesSinkParameters.class);
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
        final TupleTag<MElement> failureTag = new TupleTag<>(){};

        final PCollectionTuple outputs = input
                .apply("WriteFile", ParDo
                        .of(new CopyDoFn(getJobName(), getName(), inputSchema, parameters, getFailFast(), failureTag))
                        .withOutputTags(outputTag, TupleTagList.of(failureTag)));

        return MCollectionTuple
                .of(outputs.get(outputTag), createOutputSchema())
                .failure(outputs.get(failureTag));
    }

    private static class CopyDoFn extends DoFn<MElement, MElement> {

        private final String jobName;
        private final String name;
        private final Schema inputSchema;

        private final String source;
        private final String destination;
        private final Map<String, String> attributes;

        private final TupleTag<MElement> failureTag;
        private final Boolean failFast;

        private transient Template templateSource;
        private transient Template templateDestination;
        private transient Map<String,Template> templateAttributes;

        private transient Set<String> templateArgs;

        CopyDoFn(
                final String jobName,
                final String name,
                final Schema inputSchema,
                final FilesSinkParameters parameters,
                final Boolean failFast,
                final TupleTag<MElement> failureTag) {

            this.jobName = jobName;
            this.name = name;
            this.inputSchema = inputSchema;

            this.source = parameters.source;
            this.destination = parameters.destination;
            this.attributes = parameters.attributes;

            this.failureTag = failureTag;
            this.failFast = failFast;
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


    }

    private static Schema createOutputSchema() {
        return Schema.builder()
                .withField("source", Schema.FieldType.STRING.withNullable(true))
                .withField("destination", Schema.FieldType.STRING.withNullable(true))
                .build();
    }

}
