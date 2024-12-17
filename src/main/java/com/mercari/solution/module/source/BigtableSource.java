package com.mercari.solution.module.source;

import com.google.bigtable.v2.*;
import com.google.bigtable.v2.Row;
import com.google.cloud.bigtable.data.v2.models.*;
import com.google.gson.JsonElement;
import com.google.protobuf.ByteString;
import com.mercari.solution.module.*;
import com.mercari.solution.util.DateTimeUtil;
import com.mercari.solution.util.gcp.BigtableUtil;
import com.mercari.solution.util.schema.BigtableSchemaUtil;
import org.apache.beam.sdk.io.gcp.bigtable.BigtableIO;
import org.apache.beam.sdk.io.range.ByteKeyRange;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
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
        private JsonElement filter;
        private JsonElement keyRange;
        private List<BigtableSchemaUtil.ColumnFamilyProperties> columns;
        private BigtableSchemaUtil.Format format;
        private BigtableSchemaUtil.CellType cellType;

        // additional fields
        private Boolean withRowKey;
        private Boolean withFirstTimestamp;
        private Boolean withLastTimestamp;
        private String rowKeyField;
        private String firstTimestampField;
        private String lastTimestampField;

        private OutputType outputType;

        // for changeStream
        private ChangeStreamParameter changeStream;

        // performance tuning
        private String appProfileId;
        private Integer maxBufferElementCount;


        private void validate(final Mode mode) {

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

            if(Mode.changeDataCapture.equals(mode)) {
                if(changeStream == null) {
                    errorMessages.add("parameters.changeStream must not be null if mode is changeStream");
                } else {
                    errorMessages.addAll(changeStream.validate(this));
                }
            }

            if (!errorMessages.isEmpty()) {
                throw new IllegalModuleException(errorMessages);
            }
        }

        private void setDefaults() {
            if(format == null) {
                format = BigtableSchemaUtil.Format.bytes;
            }
            if(cellType == null) {
                cellType = BigtableSchemaUtil.CellType.last;
            }
            if(columns == null) {
                columns = new ArrayList<>();
            }
            for(var column : columns) {
                column.setDefaults(format, cellType);
            }
            if(withRowKey == null) {
                withRowKey = false;
            }
            if(withFirstTimestamp == null) {
                withFirstTimestamp = false;
            }
            if(withLastTimestamp == null) {
                withLastTimestamp = false;
            }
            if(rowKeyField == null) {
                rowKeyField = "__rowKey";
            }
            if(firstTimestampField == null) {
                firstTimestampField = "__firstTimestamp";
            }
            if(lastTimestampField == null) {
                lastTimestampField = "__lastTimestamp";
            }
            if(outputType == null) {
                outputType = OutputType.row;
            }
        }

        private void setup() {
            if(columns != null) {
                for(var column : columns) {
                    column.setupSource();
                }
            }
        }

        private static class ChangeStreamParameter implements Serializable {

            private String changeStreamName;
            private String metadataProjectId;
            private String metadataInstanceId;
            private String metadataTableId;
            private String startTime;

            public List<String> validate(BigtableSourceParameters parentParameters) {
                final List<String> errorMessages = new ArrayList<>();
                if(this.changeStreamName == null) {
                    errorMessages.add("parameters.changeStream.changeStreamName must not be null");
                }
                return errorMessages;
            }

            public void setDefaults(BigtableSourceParameters parentParameters) {
                if(this.metadataProjectId == null) {
                    this.metadataProjectId = parentParameters.projectId;
                }
                if(this.metadataInstanceId == null) {
                    this.metadataInstanceId = parentParameters.instanceId;
                }
                if(this.metadataTableId == null) {
                    this.metadataTableId = parentParameters.tableId;
                }
            }

        }

    }

    private enum OutputType {
        row,
        cell
    }

    @Override
    public MCollectionTuple expand(PBegin begin) {
        final BigtableSourceParameters parameters = getParameters(BigtableSourceParameters.class);
        parameters.validate(getMode());
        parameters.setDefaults();
        parameters.setup();

        switch (getMode()) {
            case batch -> {
                final BigtableIO.Read read = createRead(parameters);
                final PCollection<com.google.bigtable.v2.Row> rows = begin
                        .apply("Read", read);

                final PCollection<MElement> output;
                Schema outputSchema;
                switch (parameters.outputType) {
                    case row -> {
                        output = rows
                                .apply("ConvertRow", ParDo.of(new RowToRowElementDoFn(
                                        parameters, getTimestampAttribute())));
                        outputSchema = BigtableSchemaUtil.createSchema(parameters.columns);
                        if(parameters.withRowKey) {
                            outputSchema = Schema.builder(outputSchema).withField(parameters.rowKeyField, Schema.FieldType.STRING).build();
                        }
                        if(parameters.withFirstTimestamp) {
                            outputSchema = Schema.builder(outputSchema).withField(parameters.firstTimestampField, Schema.FieldType.TIMESTAMP).build();
                        }
                        if(parameters.withLastTimestamp) {
                            outputSchema = Schema.builder(outputSchema).withField(parameters.lastTimestampField, Schema.FieldType.TIMESTAMP).build();
                        }
                    }
                    case cell -> {
                        output = rows
                                .apply("ConvertCell", ParDo.of(new RowToCellElementDoFn()));
                        outputSchema = BigtableSchemaUtil.createCellSchema();
                    }
                    default -> throw new IllegalArgumentException();
                }

                return MCollectionTuple
                        .of(output, outputSchema);
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

        if(parameters.keyRange != null && !parameters.keyRange.isJsonNull()) {
            final List<ByteKeyRange> keyRanges = BigtableUtil.createKeyRanges(parameters.keyRange);
            read = read.withKeyRanges(keyRanges);
        }
        if(parameters.filter != null && !parameters.filter.isJsonNull()) {
            final RowFilter rowFilter = BigtableUtil.createRowFilter(parameters.filter);
            read = read.withRowFilter(rowFilter);
        }

        return read;
    }

    private static BigtableIO.ReadChangeStream createReadChangeStreams(
            final BigtableSourceParameters parameters) {

        BigtableIO.ReadChangeStream readChangeStream = BigtableIO.readChangeStream()
                .withProjectId(parameters.projectId)
                .withInstanceId(parameters.instanceId)
                .withTableId(parameters.tableId)
                .withChangeStreamName(parameters.changeStream.changeStreamName)
                .withMetadataTableProjectId(parameters.changeStream.metadataProjectId)
                .withMetadataTableInstanceId(parameters.changeStream.metadataInstanceId)
                .withMetadataTableTableId(parameters.changeStream.metadataTableId);

        if(parameters.changeStream.startTime != null) {
            readChangeStream = readChangeStream.withStartTime(DateTimeUtil.toJodaInstant(parameters.changeStream.startTime));
        }

        if(parameters.appProfileId != null) {
            readChangeStream = readChangeStream.withAppProfileId(parameters.appProfileId);
        }

        return readChangeStream;
    }

    private static class RowToRowElementDoFn extends DoFn<Row, MElement> {

        private final Map<String, BigtableSchemaUtil.ColumnFamilyProperties> columns;
        private final String timestampAttribute;

        private final Boolean withFirstTimestamp;
        private final Boolean withLastTimestamp;
        private final String rowKeyField;
        private final String firstTimestampField;
        private final String lastTimestampField;


        RowToRowElementDoFn(
                final BigtableSourceParameters parameters,
                final String timestampAttribute) {

            this.columns = BigtableSchemaUtil.toMap(parameters.columns);
            this.timestampAttribute = timestampAttribute;

            this.withFirstTimestamp = parameters.withFirstTimestamp;
            this.withLastTimestamp = parameters.withLastTimestamp;
            this.rowKeyField = parameters.rowKeyField;
            this.firstTimestampField = parameters.firstTimestampField;
            this.lastTimestampField = parameters.lastTimestampField;
        }

        @Setup
        public void setup() {
            for(var kv : columns.entrySet()) {
                kv.getValue().setupSource();
            }
        }

        @ProcessElement
        public void processElement(ProcessContext c) {
            final Row row = c.element();
            if(row == null) {
                return;
            }
            final Map<String, Object> primitiveValues = BigtableSchemaUtil.toPrimitiveValues(row, columns);
            primitiveValues.put(rowKeyField, row.getKey().toStringUtf8());
            if(withFirstTimestamp || withLastTimestamp) {
                final KV<Long, Long> timestamps = BigtableSchemaUtil.getRowMinMaxTimestamps(row);
                primitiveValues.put(firstTimestampField, timestamps.getKey());
                primitiveValues.put(lastTimestampField, timestamps.getValue());
            }

            if(timestampAttribute != null) {
                if(!primitiveValues.containsKey(timestampAttribute)) {
                    throw new RuntimeException("timestampAttribute does not exists in values: " + primitiveValues);
                }
                final Instant timestamp = Instant.ofEpochMilli((Long)primitiveValues.get(timestampAttribute) / 1000L);
                final MElement output = MElement.of(primitiveValues, timestamp);
                c.outputWithTimestamp(output, timestamp);
            } else {
                final MElement output = MElement.of(primitiveValues, c.timestamp());
                c.output(output);
            }
        }
    }

    private static class RowToCellElementDoFn extends DoFn<Row, MElement> {

        @ProcessElement
        public void processElement(ProcessContext c) {
            final Row row = c.element();
            if(row == null) {
                return;
            }

            final ByteString rowKey = row.getKey();
            for(final Family family : row.getFamiliesList()) {
                final String familyName = family.getName();
                for(final Column column : family.getColumnsList()) {
                    final ByteString qualifier = column.getQualifier();
                    for(final Cell cell : column.getCellsList()) {
                        final Map<String, Object> primitives = new HashMap<>();
                        primitives.put("rowKey", rowKey.toStringUtf8());
                        primitives.put("family", familyName);
                        primitives.put("qualifier", qualifier.toStringUtf8());
                        primitives.put("timestamp", cell.getTimestampMicros());
                        final Instant timestamp = Instant.ofEpochMilli(cell.getTimestampMicros() / 1000L);
                        final MElement element = MElement.of(primitives, timestamp);
                        c.outputWithTimestamp(element, timestamp);
                    }
                }
            }
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
