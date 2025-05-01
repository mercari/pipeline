package com.mercari.solution.module.source;

import com.mercari.solution.module.*;
import com.mercari.solution.util.coder.ElementCoder;
import com.mercari.solution.util.pipeline.OptionUtil;
import org.apache.beam.sdk.io.iceberg.IcebergCatalogConfig;
import org.apache.beam.sdk.io.iceberg.IcebergIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.*;
import org.apache.iceberg.catalog.TableIdentifier;
import org.jetbrains.annotations.NotNull;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Source.Module(name="iceberg")
public class IcebergSource extends Source {

    private static class Parameters implements Serializable {

        private String input;
        private String catalogName;
        private Map<String, String> catalogProperties;
        private Map<String, String> configProperties;
        private Long triggeringFrequencySeconds;

        private void validate() {
            final List<String> errorMessages = new ArrayList<>();
            if(input == null) {
                errorMessages.add("parameters.input must not be null");
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
        }

    }

    public MCollectionTuple expand(
            @NotNull PBegin begin,
            MErrorHandler errorHandler) {

        final Parameters parameters = getParameters(Parameters.class);
        parameters.validate();
        parameters.setDefaults();

        final PCollection<Row> rows = begin
                .apply("Read", createRead(begin, parameters));

        final Schema schema = Schema.of(rows.getSchema());
        final PCollection<MElement> output = rows
                .apply("Format", ParDo.of(new FormatDoFn()))
                .setCoder(ElementCoder.of(schema));

        return MCollectionTuple
                .of(output, schema);

    }

    private static class FormatDoFn extends DoFn<Row, MElement> {

        @ProcessElement
        public void processElement(final ProcessContext c) {
            final Row input = c.element();
            if(input == null) {
                return;
            }
            final MElement output = MElement.of(input, c.timestamp());
            c.output(output);
        }

    }

    private static IcebergIO.ReadRows createRead(
            final PBegin begin,
            final Parameters parameters) {
        IcebergIO.ReadRows readRows = IcebergIO
                .readRows(IcebergCatalogConfig.builder()
                        .setCatalogName(parameters.catalogName)
                        .setCatalogProperties(parameters.catalogProperties)
                        .setConfigProperties(parameters.configProperties)
                        .build())
                .from(TableIdentifier.parse(parameters.input));

        if(OptionUtil.isStreaming(begin)) {
            readRows = readRows.streaming(true);
        }

        return readRows;
    }

}
