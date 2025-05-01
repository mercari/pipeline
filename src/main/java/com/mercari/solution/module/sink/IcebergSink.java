package com.mercari.solution.module.sink;

import com.mercari.solution.module.*;
import com.mercari.solution.util.TemplateUtil;
import com.mercari.solution.util.pipeline.Union;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.io.iceberg.*;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.Row;
import org.apache.iceberg.catalog.TableIdentifier;
import org.jetbrains.annotations.NotNull;
import org.joda.time.Duration;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Sink.Module(name="iceberg")
public class IcebergSink extends Sink {

    private static class Parameters implements Serializable {

        private String output;
        private String catalogName;
        private Map<String, String> catalogProperties;
        private Map<String, String> configProperties;
        private Long triggeringFrequencySeconds;

        private void validate() {
            final List<String> errorMessages = new ArrayList<>();
            if(output == null) {
                errorMessages.add("parameters.output must not be null");
            }
            if(catalogName == null) {
                errorMessages.add("parameters.catalogName must not be null");
            }
            if(!errorMessages.isEmpty()) {
                throw new IllegalModuleException(errorMessages);
            }
        }

        private void setDefaults() {
            if(catalogProperties == null) {
                catalogProperties = new HashMap<>();
            }
            if(configProperties == null) {
                configProperties = new HashMap<>();
            }
            if(!configProperties.containsKey("type")) {
                configProperties.put("type", "hadoop");
            }
        }

    }

    public MCollectionTuple expand(
            @NotNull MCollectionTuple inputs,
            MErrorHandler errorHandler) {

        final Parameters parameters = getParameters(Parameters.class);
        parameters.validate();
        parameters.setDefaults();

        final PCollection<MElement> input = inputs
                .apply("Union", Union.flatten()
                        .withWaits(getWaits())
                        .withStrategy(getStrategy()));
        final Schema inputSchema = Union.createUnionSchema(inputs);

        final IcebergWriteResult result = input
                .apply("ConvertToRow", ParDo
                        .of(new FormatDoFn(inputSchema)))
                .setCoder(RowCoder.of(inputSchema.getRowSchema()))
                .apply("Write", createWrite(parameters));

        return MCollectionTuple
                .done(PDone.in(inputs.getPipeline()));

    }

    private static class FormatDoFn extends DoFn<MElement, Row> {

        private final Schema outputSchema;

        FormatDoFn(final Schema outputSchema) {
            this.outputSchema = outputSchema.withType(DataType.ROW);
        }

        @ProcessElement
        public void processElement(final ProcessContext c) {
            final MElement input = c.element();
            if(input == null) {
                return;
            }

            try {
                final MElement output = input.convert(outputSchema);
                final Row row = (Row) output.getValue();
                //c.output(row);
            } catch (final Throwable e) {

            }
        }

    }

    private static IcebergIO.WriteRows createWrite(Parameters parameters) {
        IcebergIO.WriteRows writeRows = IcebergIO
                .writeRows(IcebergCatalogConfig.builder()
                        .setCatalogName(parameters.catalogName)
                        .setCatalogProperties(parameters.catalogProperties)
                        .setConfigProperties(parameters.configProperties)
                        .build());

        if(TemplateUtil.isTemplateText(parameters.output)) {
            // TODO
        } else {
            writeRows = writeRows.to(TableIdentifier.parse(parameters.output));
        }

        if(parameters.triggeringFrequencySeconds != null) {
            writeRows = writeRows.withTriggeringFrequency(Duration
                    .standardSeconds(parameters.triggeringFrequencySeconds));
        }

        return writeRows;
    }

}
