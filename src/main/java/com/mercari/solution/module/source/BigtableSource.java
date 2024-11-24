package com.mercari.solution.module.source;

import com.google.bigtable.v2.Row;
import com.google.bigtable.v2.RowFilter;
import com.google.cloud.bigtable.data.v2.models.*;
import com.google.gson.JsonElement;
import com.google.protobuf.ByteString;
import com.mercari.solution.module.*;
import com.mercari.solution.util.gcp.BigtableUtil;
import com.mercari.solution.util.schema.converter.BigtableRowToElementConverter;
import org.apache.beam.sdk.io.gcp.bigtable.BigtableIO;
import org.apache.beam.sdk.io.range.ByteKeyRange;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Source.Module(name="bigtable")
public class BigtableSource extends Source {

    private static final Logger LOG = LoggerFactory.getLogger(BigtableSource.class);

    private static class BigtableSourceParameters implements Serializable {

        private String projectId;
        private String instanceId;
        private String tableId;

        // for batch
        private JsonElement rowFilter;
        private JsonElement keyRanges;

        //
        private String appProfileId;
        private Integer maxBufferElementCount;


        private void validate(final PBegin begin) {

            // check required parameters filled
            final List<String> errorMessages = new ArrayList<>();

            if(this.projectId == null) {
                errorMessages.add("parameters.projectId must not be null");
            }
            if(this.instanceId == null) {
                errorMessages.add("parameters.instanceId must not be null");
            }
            if(this.tableId == null) {
                errorMessages.add("parameters.tableId must not be null");
            }

            if (!errorMessages.isEmpty()) {
                throw new IllegalModuleException(errorMessages);
            }
        }

        private void setDefaults() {
        }

    }

    private enum OutputType {
        avro,
        row,
        mutation,
        cells
    }

    @Override
    public MCollectionTuple expand(PBegin begin) {
        final BigtableSourceParameters parameters = getParameters(BigtableSourceParameters.class);
        parameters.validate(begin);
        parameters.setDefaults();

        switch (getMode()) {
            case batch -> {
                final BigtableIO.Read read = createRead(parameters);
                final PCollection<com.google.bigtable.v2.Row> rows = begin
                        .apply("Read", read);
                final PCollection<MElement> output = rows
                        .apply("Convert", ParDo.of(new RowToElementDoFn(getSchema())));

                return MCollectionTuple.of(output, getSchema());
            }
            case changeDataCapture -> {
                final BigtableIO.ReadChangeStream read = createReadChangeStreams(parameters);
                final PCollection<KV<ByteString, ChangeStreamMutation>> mutations = begin
                        .apply("ReadChangeStream", read);
                final PCollection<MElement> output = mutations
                        .apply("Convert", ParDo.of(new ChangeStreamToElementDoFn(getSchema())));
                return MCollectionTuple.of(output, getSchema());
            }
            default -> throw new IllegalArgumentException("Not supported mode: " + getMode());
        }
    }


    private static BigtableIO.ReadChangeStream createReadChangeStreams(
            final BigtableSourceParameters parameters) {
        BigtableIO.ReadChangeStream read = BigtableIO.readChangeStream()
                .withProjectId(parameters.projectId)
                .withInstanceId(parameters.instanceId)
                .withTableId(parameters.tableId);

        if(parameters.appProfileId != null) {
            read = read.withAppProfileId(parameters.appProfileId);
        }

        return read;
    }

    private static BigtableIO.Read createRead(
            BigtableSourceParameters parameters) {
        BigtableIO.Read read = BigtableIO.read()
                .withProjectId(parameters.projectId)
                .withInstanceId(parameters.instanceId)
                .withTableId(parameters.tableId);

        if(parameters.appProfileId != null) {
            read = read.withAppProfileId(parameters.appProfileId);
        }
        if(parameters.maxBufferElementCount != null) {
            read = read.withMaxBufferElementCount(parameters.maxBufferElementCount);
        }

        if(parameters.keyRanges != null && !parameters.keyRanges.isJsonNull()) {
            final List<ByteKeyRange> keyRanges = BigtableUtil.createKeyRanges(parameters.keyRanges);
            read = read.withKeyRanges(keyRanges);
        }
        if(parameters.rowFilter != null && !parameters.rowFilter.isJsonNull()) {
            final RowFilter rowFilter = BigtableUtil.createRowFilter(parameters.rowFilter);
            read = read.withRowFilter(rowFilter);
        }

        return read;
    }

    private static class RowToElementDoFn extends DoFn<Row, MElement> {

        private final Schema schema;

        RowToElementDoFn(final Schema schema) {
            this.schema = schema;
        }

        @ProcessElement
        public void processElement(ProcessContext c) {
            final Row row = c.element();
            final Map<String, Object> values = BigtableRowToElementConverter.convert(schema, row);
            final MElement output = MElement.of(values, c.timestamp());
            c.output(output);
        }
    }

    private static class ChangeStreamToElementDoFn extends DoFn<KV<ByteString, ChangeStreamMutation>, MElement> {

        private final Schema schema;

        ChangeStreamToElementDoFn(final Schema schema) {
            this.schema = schema;
        }

        @ProcessElement
        public void processElement(ProcessContext c) {
            final KV<ByteString, ChangeStreamMutation> kv = c.element();
            if(kv == null) {
                return;
            }
            final ByteString rowKey = kv.getKey();
            final ChangeStreamMutation mutation = kv.getValue();
            if(rowKey == null || mutation == null) {
                return;
            }
            for(final Entry entry : mutation.getEntries()) {
                switch (entry) {
                    case SetCell set -> {

                    }
                    case AddToCell add -> {

                    }
                    case MergeToCell merge -> {

                    }
                    case DeleteCells deleteCells -> {

                    }
                    case DeleteFamily deleteFamily -> {

                    }
                    default -> throw new RuntimeException();
                };
            }
        }
    }

}
