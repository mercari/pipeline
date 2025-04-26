package com.mercari.solution.module.sink;

import com.mercari.solution.MPipeline;
import com.mercari.solution.module.*;
import com.mercari.solution.util.coder.ElementCoder;
import com.mercari.solution.util.pipeline.Union;
import com.mercari.solution.util.pipeline.action.Action;
import com.mercari.solution.util.pipeline.action.BigQueryAction;
import com.mercari.solution.util.pipeline.action.DataflowAction;
import com.mercari.solution.util.pipeline.action.vertexai.GeminiAction;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.errorhandling.BadRecord;
import org.apache.beam.sdk.values.*;

import java.io.Serializable;
import java.util.*;

@Sink.Module(name="action")
public class ActionSink extends Sink {

    private static class Parameters implements Serializable {

        private Action.Service service;
        private DataflowAction.Parameters dataflow;
        private BigQueryAction.Parameters bigquery;
        private GeminiAction.Parameters gemini;

        private Map<String, String> labels;

        public Action.Service getService() {
            return service;
        }

        private void validate(final String name) {
            final List<String> errorMessages = new ArrayList<>();
            if(this.service == null) {
                errorMessages.add("action sink[" + name + "].parameters.service must not be null");
            } else {
                final List<String> missingServiceParametersErrors = switch (this.service) {
                    case dataflow -> Optional.ofNullable(this.dataflow).isEmpty() ?
                            List.of("action sink[" + name + "].parameters.dataflow must not be null") : this.dataflow.validate(name);
                    case bigquery -> Optional.ofNullable(this.bigquery).isEmpty() ?
                            List.of("action sink[" + name + "].parameters.bigquery must not be null") : this.bigquery.validate(name);
                    case vertexai_gemini -> Optional.ofNullable(this.gemini).isEmpty() ?
                            List.of("action sink[" + name + "].parameters.gemini must not be null") : this.gemini.validate(name);
                    default -> throw new IllegalArgumentException();
                };
                errorMessages.addAll(missingServiceParametersErrors);
            }

            if(!errorMessages.isEmpty()) {
                throw new IllegalModuleException(errorMessages);
            }
        }

        private void setDefaults(final MPipeline.MPipelineOptions options) {
            switch (this.service) {
                case dataflow -> this.dataflow.setDefaults(options);
            }
        }

    }

    @Override
    public MCollectionTuple expand(
            final MCollectionTuple inputs,
            final MErrorHandler errorHandler) {

        final Pipeline pipeline = inputs.getPipeline();
        final Parameters parameters = getParameters(Parameters.class);
        parameters.validate(getName());
        parameters.setDefaults(pipeline.getOptions().as(MPipeline.MPipelineOptions.class));

        final PCollection<MElement> input;
        if(inputs.size() > 0) {
            input = inputs
                    .apply("Union", Union.flatten()
                            .withWaits(getWaits())
                            .withStrategy(getStrategy()));
        } else {
            final Schema inputSchema = MElement.dummySchema();
            input = inputs.getPipeline().begin()
                    .apply("Seed", Create
                            .of("")
                            .withCoder(StringUtf8Coder.of()))
                    .apply("Dummy", ParDo.of(new DummyInputDoFn()))
                    .setCoder(ElementCoder.of(inputSchema))
                    .apply("Wait", Wait
                            .on(getWaits()))
                    .setCoder(ElementCoder.of(inputSchema));
        }

        final Action action = switch (parameters.getService()) {
            case dataflow -> DataflowAction.of(parameters.dataflow, parameters.labels);
            case bigquery -> BigQueryAction.of(parameters.bigquery);
            case vertexai_gemini -> GeminiAction.of(parameters.gemini);
            default -> throw new IllegalArgumentException("Not supported service: " + parameters.getService());
        };

        final Schema outputSchema = action.getOutputSchema();
        final TupleTag<MElement> outputTag = new TupleTag<>() {};
        final TupleTag<BadRecord> failureTag = new TupleTag<>() {};

        final PCollectionTuple outputs = input
                .apply("Action", ParDo
                        .of(new ActionDoFn(getJobName(), getName(), action, getFailFast(), failureTag))
                        .withOutputTags(outputTag, TupleTagList.of(failureTag)));

        return MCollectionTuple
                .of(outputs.get(outputTag), outputSchema);
    }

    private static class ActionDoFn extends DoFn<MElement, MElement> {

        private final String jobName;
        private final String name;

        private final Action action;

        private final boolean failFast;
        private final TupleTag<BadRecord> failuresTag;


        ActionDoFn(
                final String jobName,
                final String name,
                final Action action,
                final boolean failFast,
                final TupleTag<BadRecord> failuresTag) {

            this.jobName = jobName;
            this.name = name;
            this.action = action;
            this.failFast = failFast;
            this.failuresTag = failuresTag;
        }

        @Setup
        public void setup() {
            this.action.setup();
        }

        @ProcessElement
        public void processElement(final ProcessContext c) {
            final MElement input = c.element();
            if(input == null) {
                return;
            }
            try {
                final MElement output = this.action.action();
                c.output(output);
            } catch (final Throwable e) {
                final BadRecord badRecord = processError("Failed to execute sink.action", input, e, failFast);
                c.output(failuresTag, badRecord);
            }
        }

    }

    private static class DummyInputDoFn extends DoFn<String, MElement> {

        @ProcessElement
        public void processElement(final ProcessContext c) {
            final MElement element = MElement.createDummyElement(c.timestamp());
            c.output(element);
        }

    }

}
