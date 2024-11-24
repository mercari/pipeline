package com.mercari.solution.module.sink;

import com.mercari.solution.MPipeline;
import com.mercari.solution.module.*;
import com.mercari.solution.util.coder.ElementCoder;
import com.mercari.solution.util.pipeline.action.Action;
import com.mercari.solution.util.pipeline.action.BigQueryAction;
import com.mercari.solution.util.pipeline.action.DataflowAction;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.*;

@Sink.Module(name="action")
public class ActionSink extends Sink {

    private static final Logger LOG = LoggerFactory.getLogger(ActionSink.class);

    private static class ActionSinkParameters implements Serializable {

        private Action.Service service;
        private DataflowAction.Parameters dataflow;
        private BigQueryAction.Parameters bigquery;

        private Map<String, String> labels;

        public Action.Service getService() {
            return service;
        }

        public DataflowAction.Parameters getDataflow() {
            return dataflow;
        }

        public BigQueryAction.Parameters getBigquery() {
            return bigquery;
        }

        public Map<String, String> getLabels() {
            return labels;
        }

        private void validate(final String name) {
            final List<String> errorMessages = new ArrayList<>();
            if(this.service == null) {
                errorMessages.add("action sink[" + name + "].parameters.service must not be null");
            } else {
                final List<String> missingServiceParametersErrors = switch (this.service) {
                    case dataflow -> Optional.ofNullable(this.dataflow).isEmpty() ?
                            List.of("action sink[" + name + "].parameters.dataflow must not be null") : this.dataflow.validate(name);
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
    public MCollectionTuple expand(MCollectionTuple inputs) {
        final Pipeline pipeline = inputs.getPipeline();
        final ActionSinkParameters parameters = getParameters(ActionSinkParameters.class);
        parameters.validate(getName());
        parameters.setDefaults(pipeline.getOptions().as(MPipeline.MPipelineOptions.class));

        final PCollection<String> seed = inputs.getPipeline().begin()
                .apply("Seed", Create
                        .of("")
                        .withCoder(StringUtf8Coder.of()));

        final Action action = switch (parameters.getService()) {
            case dataflow -> DataflowAction.of(parameters.getDataflow(), parameters.getLabels());
            case bigquery -> BigQueryAction.of(parameters.getBigquery());
            default -> throw new IllegalArgumentException("Not supported service: " + parameters.getService());
        };

        final Schema outputSchema = action.getOutputSchema();

        final PCollection<?> output = seed
                .apply("Wait", Wait.on(getWaits()))
                .setCoder(seed.getCoder())
                .apply("Action", ParDo.of(new ActionDoFn<>(getName(), action)))
                .setCoder(ElementCoder.of(outputSchema));
        return null;
    }

    private static class ActionDoFn<T> extends DoFn<T, MElement> {

        private final String name;
        private final Action action;

        ActionDoFn(
                final String name,
                final Action action) {

            this.name = name;
            this.action = action;
        }

        @Setup
        public void setup() {
            this.action.setup();
        }

        @ProcessElement
        public void processElement(final ProcessContext c) throws IOException {
            this.action.action();
            //c.output("");
        }

    }

}
