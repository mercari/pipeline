package com.mercari.solution.util.schema;

import com.google.bigtable.v2.*;
import com.google.bigtable.v2.Mutation;
import com.google.bigtable.v2.Row;
import com.google.bigtable.v2.Value;
import com.google.cloud.ByteArray;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.models.*;
import com.google.cloud.bigtable.data.v2.models.sql.*;
import com.google.cloud.bigtable.data.v2.models.sql.ColumnMetadata;
import com.google.cloud.bigtable.data.v2.models.sql.ResultSetMetadata;
import com.google.protobuf.ByteString;
import com.mercari.solution.module.MElement;
import com.mercari.solution.module.Schema;
import com.mercari.solution.util.DateTimeUtil;
import com.mercari.solution.util.FailureUtil;
import com.mercari.solution.util.TemplateUtil;
import freemarker.template.Template;
import org.apache.avro.util.Utf8;
import org.apache.beam.sdk.values.KV;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.*;
import org.joda.time.Instant;

import java.io.*;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;

public class BigtableSchemaUtil {

    private static final String RESOURCE_CDC_AVRO_SCHEMA_PATH = "/schema/avro/bigtable_cdc.avsc";
    private static final String RESOURCE_RUNTIME_CDC_AVRO_SCHEMA_PATH = "/template/MPipeline/resources/schema/avro/bigtable_cdc.avsc";

    public enum Format {
        bytes,
        avro,
        text,
        hadoop,
        avromap
    }

    public enum MutationOp implements Serializable {
        SET_CELL,
        ADD_TO_CELL,
        MERGE_TO_CELL,
        DELETE_FROM_COLUMN,
        DELETE_FROM_FAMILY,
        DELETE_FROM_ROW
    }

    public enum TimestampType implements Serializable {
        server,
        event,
        current,
        field,
        fixed,
        zero
    }

    public enum CellType implements Serializable {
        all,
        first,
        last
    }

    public enum ModType {

        SET_CELL(0),
        DELETE_FAMILY(1),
        DELETE_CELLS(2),
        UNKNOWN(3);

        private final int id;

        ModType(int id) {
            this.id = id;
        }

        public int getId() {
            return id;
        }
    }

    public static class ColumnFamilyProperties implements Serializable {

        private String family;
        private List<ColumnQualifierProperties> qualifiers;
        private Format format;

        // for sink
        private MutationOp mutationOp;
        private TimestampType timestampType;
        private String timestampField;
        private String timestampValue;

        // for source
        private CellType cellType;

        private transient Template templateFamily;

        public List<String> validate(int i) {
            final List<String> errorMessages = new ArrayList<>();
            if(family == null) {
                errorMessages.add("parameters.columns[" + i + "].family must not be null");
            }
            if(qualifiers == null || qualifiers.isEmpty()) {
                if(!MutationOp.DELETE_FROM_FAMILY.equals(mutationOp)) {
                    errorMessages.add("parameters.columns[" + i + "].qualifiers must not be empty");
                }
            } else {
                for(int j=0; j<qualifiers.size(); j++) {
                    errorMessages.addAll(qualifiers.get(j).validate(i, j));
                }
            }
            return errorMessages;
        }

        // for source read and transform read
        public void setDefaults(
                final Format defaultFormat,
                final CellType cellType) {

            setDefaults(defaultFormat, null, null, null, null, cellType, null);
        }

        // for sink write cell
        public void setDefaults(
                final Format defaultFormat,
                final MutationOp defaultOp,
                final TimestampType defaultTimestampType,
                final String defaultTimestampField,
                final String defaultTimestampValue,
                final List<Schema.Field> fields) {

            setDefaults(defaultFormat, defaultOp,
                    defaultTimestampType, defaultTimestampField, defaultTimestampValue,
                    null, fields);
        }

        // for read cell
        private void setDefaults(
                final Format defaultFormat,
                final MutationOp defaultOp,
                final TimestampType defaultTimestampType,
                final String defaultTimestampField,
                final String defaultTimestampValue,
                final CellType defaultCellType,
                final List<Schema.Field> fields) {

            if(format == null) {
                format = defaultFormat;
            }
            if(mutationOp == null) {
                mutationOp = Optional
                        .ofNullable(defaultOp)
                        .orElse(MutationOp.SET_CELL);
            }
            if(timestampType == null) {
                timestampType = Optional
                        .ofNullable(defaultTimestampType)
                        .orElse(TimestampType.server);
            }
            if(timestampField == null) {
                timestampField = defaultTimestampField;
            }
            if(timestampValue == null) {
                timestampValue = defaultTimestampValue;
            }
            if(cellType == null) {
                cellType = Optional.ofNullable(defaultCellType).orElse(CellType.last);
            }
            if(qualifiers == null) {
                qualifiers = new ArrayList<>();
                switch (mutationOp) {
                    case SET_CELL, DELETE_FROM_COLUMN -> {
                        for(final Schema.Field field : fields) {
                            final ColumnQualifierProperties qualifier = ColumnQualifierProperties.of(field);
                            qualifiers.add(qualifier);
                        }
                    }
                }
            }
            for(final ColumnQualifierProperties qualifier : qualifiers) {
                qualifier.setDefaults(format, mutationOp, timestampType, timestampField, timestampValue);
            }
        }

        public List<String> extractValueArgs() {
            final List<String> valueArgs = new ArrayList<>();
            for(final ColumnQualifierProperties qualifier : qualifiers) {
                valueArgs.add(qualifier.field);
                if(qualifier.timestampField != null) {
                    valueArgs.add(qualifier.timestampField);
                }
            }
            return valueArgs;
        }

        public List<String> extractTemplateArgs(final Schema inputSchema) {
            final List<String> templateArgs = TemplateUtil.extractTemplateArgs(family, inputSchema);
            for(final ColumnQualifierProperties qualifier : qualifiers) {
                templateArgs.addAll(qualifier.extractTemplateArgs(inputSchema));
            }
            return templateArgs;
        }

        public void setupSource() {
            this.templateFamily = TemplateUtil.createStrictTemplate("templateColumnFamily", family);
            for(final ColumnQualifierProperties qualifier : qualifiers) {
                qualifier.setupSource();
            }
        }

        public void setupSink() {
            this.templateFamily = TemplateUtil.createStrictTemplate("templateColumnFamily", family);
            for(final ColumnQualifierProperties qualifier : qualifiers) {
                qualifier.setupSink();
            }
        }

        private List<Mutation> toMutation(
                final Map<String, Object> primitiveValues,
                final Map<String, Object> standardValues,
                final Instant timestamp) {

            final String cf = TemplateUtil.executeStrictTemplate(templateFamily, standardValues);
            final List<Mutation> mutations = new ArrayList<>();
            if(MutationOp.DELETE_FROM_FAMILY.equals(mutationOp)) {
                final Mutation mutation = Mutation.newBuilder()
                        .setDeleteFromFamily(Mutation.DeleteFromFamily.newBuilder()
                                .setFamilyName(cf)
                                .build())
                        .build();
                mutations.add(mutation);
            } else {
                for(final ColumnQualifierProperties qualifier : qualifiers) {
                    final Mutation mutation = qualifier.toMutation(cf, primitiveValues, standardValues, timestamp);
                    if(mutation == null) {
                        continue;
                    }
                    mutations.add(mutation);
                }
            }
            return mutations;
        }

        private Map<String, Object> toElement(final Family family) {
            final Map<String, Object> primitiveValues = new HashMap<>();
            for(final Column column : family.getColumnsList()) {
                for(final ColumnQualifierProperties qualifierProperty : qualifiers) {
                    if(qualifierProperty.name.equals(column.getQualifier().toStringUtf8())) {
                        final List<Object> values = qualifierProperty.toPrimitiveValues(column);
                        if(values.isEmpty()) {
                            continue;
                        }
                        final Object cellValue = switch (cellType) {
                            case all -> values;
                            case last -> values.getFirst();
                            case first -> values.getLast();
                        };
                        primitiveValues.put(qualifierProperty.field, cellValue);
                        break;
                    }
                }
            }
            return primitiveValues;
        }

        @Override
        public String toString() {
            final String qualifiersString;
            if(qualifiers != null) {
                qualifiersString = qualifiers.stream().map(ColumnQualifierProperties::toString).collect(Collectors.joining(","));
            } else {
                qualifiersString = null;
            }
            return String.format("family: %s, qualifiers: %s", family, qualifiersString);
        }

    }

    public static class ColumnQualifierProperties implements Schema.IField {

        private String name;
        private String field;
        private Format format;

        // for sink
        private MutationOp mutationOp;
        private TimestampType timestampType;
        private String timestampField;
        private String timestampValue;

        // for source
        private CellType cellType;
        // schema
        private String type;
        private String mode;
        private List<ColumnQualifierProperties> fields;
        private List<String> symbols;
        private String valueType;

        private Schema.FieldType fieldType;

        private transient Template templateQualifier;
        private transient long fixedTimestampMicros;

        @Override
        public String toString() {
            return String.format("{ name: %s, field: %s, format: %s }", name, field, format);
        }

        @Override
        public String getName() {
            return name;
        }

        @Override
        public String getType() {
            return type;
        }

        @Override
        public String getMode() {
            return mode;
        }

        @Override
        public List<ColumnQualifierProperties> getFields() {
            return fields;
        }

        @Override
        public List<String> getSymbols() {
            return symbols;
        }

        @Override
        public String getValueType() {
            return valueType;
        }

        public static ColumnQualifierProperties of(final Schema.Field field) {
            final ColumnQualifierProperties qualifier = new ColumnQualifierProperties();
            qualifier.name = field.getName();
            qualifier.field = field.getName();
            return qualifier;
        }

        public List<String> validate(int i, int j) {
            final List<String> errorMessages = new ArrayList<>();
            if(name == null) {
                errorMessages.add("parameters.columns[" + i + "].qualifiers[" + j + "].name must not be null");
            }
            if(field == null && name == null) {
                errorMessages.add("parameters.columns[" + i + "].qualifiers[" + j + "].field must not be empty");
            }
            if(symbols == null) {
                symbols = new ArrayList<>();
            }
            if(timestampType != null) {
                switch (timestampType) {
                    case field -> {
                        if(timestampField == null) {
                            errorMessages.add("parameters.columns[" + i + "].qualifiers[" + j + "].timestampField must not be empty if timestampType is field");
                        }
                    }
                    case fixed -> {
                        if(timestampValue == null) {
                            errorMessages.add("parameters.columns[" + i + "].qualifiers[" + j + "].timestampValue must not be empty if timestampType is fixed");
                        }
                    }
                }
            }
            return errorMessages;
        }

        // for write
        public void setDefaults(
                final Format defaultFormat,
                final MutationOp defaultOp,
                final TimestampType defaultTimestampType,
                final String defaultTimestampField,
                final String defaultTimestampValue) {

            setDefaults(defaultFormat, defaultOp,
                    defaultTimestampType, defaultTimestampField, defaultTimestampValue,
                    null);
        }

        // for read
        private void setDefaults(
                final Format defaultFormat,
                final MutationOp defaultOp,
                final TimestampType defaultTimestampType,
                final String defaultTimestampField,
                final String defaultTimestampValue,
                final CellType defaultCellType) {

            if(field == null) {
                field = name;
            }
            if(format == null) {
                format = defaultFormat;
            }
            if(mutationOp == null) {
                mutationOp = defaultOp;
            }
            if(timestampType == null) {
                timestampType = defaultTimestampType;
            }
            if(this.timestampField == null) {
                this.timestampField = defaultTimestampField;
            }
            if(this.timestampValue == null) {
                this.timestampValue = defaultTimestampValue;
            }
            if(cellType == null) {
                cellType = defaultCellType;
            }
        }

        public List<String> extractTemplateArgs(final Schema inputSchema) {
            return TemplateUtil.extractTemplateArgs(name, inputSchema);
        }

        public void setupSource() {
            this.fieldType = Schema.IField.toFieldType(this);
            this.templateQualifier = TemplateUtil.createStrictTemplate("templateQualifier", name);
        }

        public void setupSink() {
            this.templateQualifier = TemplateUtil.createStrictTemplate("templateQualifier", name);
        }

        private Mutation toMutation(
                final String cf,
                final Map<String, Object> primitiveValues,
                final Map<String, Object> standardValues,
                final Instant timestamp) {

            final Object primitiveValue = primitiveValues.get(field);
            if(primitiveValue == null) {
                return null;
            }

            final String cq = TemplateUtil.executeStrictTemplate(templateQualifier, standardValues);
            return switch (mutationOp) {
                case SET_CELL -> {
                    final ByteString fieldValue = toByteString(format, primitiveValue);
                    final long timestampMicros = switch (timestampType) {
                        case server -> -1L;
                        case event -> timestamp.getMillis() * 1000L;
                        case current -> DateTimeUtil.reduceAccuracy(DateTimeUtil.toEpochMicroSecond(java.time.Instant.now()), 1000L);
                        case field -> DateTimeUtil.reduceAccuracy((Long) primitiveValues.get(timestampField), 1000L);
                        case fixed -> DateTimeUtil.toEpochMicroSecond(timestampValue);
                        case zero -> 0L;
                    };
                    final Mutation.SetCell cell = Mutation.SetCell.newBuilder()
                            .setFamilyName(cf)
                            .setColumnQualifier(ByteString.copyFrom(cq, StandardCharsets.UTF_8))
                            .setValue(fieldValue)
                            .setTimestampMicros(timestampMicros >= -1 ? timestampMicros : -1)
                            .build();
                    yield Mutation.newBuilder().setSetCell(cell).build();
                }
                case ADD_TO_CELL -> {
                    final long timestampMicros = switch (timestampType) {
                        case server -> -1L;
                        case event -> timestamp.getMillis() * 1000L;
                        case current -> DateTimeUtil.reduceAccuracy(DateTimeUtil.toEpochMicroSecond(java.time.Instant.now()), 1000L);
                        case field -> DateTimeUtil.reduceAccuracy((Long) primitiveValues.get(timestampField), 1000L);
                        case fixed -> DateTimeUtil.toEpochMicroSecond(timestampValue);
                        case zero -> 0L;
                    };
                    final Mutation.AddToCell cell = Mutation.AddToCell.newBuilder()
                            .setFamilyName(cf)
                            .setColumnQualifier(Value.newBuilder().setBytesValue(ByteString.copyFrom(cq, StandardCharsets.UTF_8)))
                            .setInput(toValue(primitiveValue))
                            .setTimestamp(Value.newBuilder().setTimestampValue(DateTimeUtil.toProtoTimestamp(timestampMicros)))
                            .build();
                    yield Mutation.newBuilder().setAddToCell(cell).build();
                }
                case MERGE_TO_CELL -> {
                    final Mutation.MergeToCell cell = Mutation.MergeToCell.newBuilder()
                            .setFamilyName(cf)
                            .setColumnQualifier(Value.newBuilder().setBytesValue(ByteString.copyFrom(cq, StandardCharsets.UTF_8)))
                            .setInput(toValue(primitiveValue))
                            .setTimestamp(Value.newBuilder().setTimestampValue(DateTimeUtil.toProtoTimestamp(1L)))
                            .build();
                    yield Mutation.newBuilder().setMergeToCell(cell).build();
                }
                case DELETE_FROM_COLUMN -> Mutation.newBuilder()
                        .setDeleteFromColumn(Mutation.DeleteFromColumn.newBuilder()
                            .setFamilyName(cf)
                            .setColumnQualifier(ByteString.copyFrom(cq, StandardCharsets.UTF_8))
                            .build())
                        .build();
                default -> throw new IllegalArgumentException("Illegal mutationOp: " + mutationOp + " for columnQualifier");
            };
        }

        private List<Object> toPrimitiveValues(final Column column) {
            final List<Object> list = new ArrayList<>();
            for(final Cell c : column.getCellsList()) {
                final Object primitiveValue = BigtableSchemaUtil.toPrimitiveValue(format, fieldType, c.getValue());
                list.add(primitiveValue);
            }
            return list;
        }

        private Object toPrimitiveValue(final ByteString byteString) {
            return BigtableSchemaUtil.toPrimitiveValue(format, fieldType, byteString);
        }
    }

    public static Schema createSchema(final List<ColumnFamilyProperties> families) {
        final List<Schema.Field> fields = families.stream()
                .flatMap(f -> f.qualifiers.stream())
                .peek(ColumnQualifierProperties::setupSource)
                .map(q -> Schema.Field.of(q.field, q.fieldType))
                .toList();
        return Schema.builder().withFields(fields).build();
    }

    public static Schema createCellSchema() {
        return Schema.builder()
                .withField("rowKey", Schema.FieldType.STRING)
                .withField("family", Schema.FieldType.STRING)
                .withField("qualifier", Schema.FieldType.STRING)
                .withField("value", Schema.FieldType.BYTES)
                .withField("timestamp", Schema.FieldType.TIMESTAMP)
                .build();
    }

    public static Schema convertSchema(final ResultSetMetadata meta)  throws SQLException {
        final Schema.Builder builder = Schema.builder();
        for (final ColumnMetadata columnMetadata : meta.getColumns()) {
            builder.withField(columnMetadata.name(), convertFieldType(columnMetadata.type()));
        }
        return builder.build();
    }

    public static Schema convertSchema(final String projectId, final String instanceId, final String sql) {
        try(final BigtableDataClient client = BigtableDataClient.create(projectId, instanceId);
            final ResultSet resultSet = client.executeQuery(Statement.newBuilder(sql).build())) {
            return BigtableSchemaUtil.convertSchema(resultSet.getMetadata());
        } catch (final IOException | SQLException e) {
            throw new RuntimeException("", e);
        }
    }

    public static MElement convert(
            final ResultSet resultSet,
            final Instant timestamp) {

        final Map<String, Object> primitiveValues = new HashMap<>();
        final ResultSetMetadata meta = resultSet.getMetadata();
        for (final ColumnMetadata columnMetadata : meta.getColumns()) {
            final Object primitiveValue = convertFieldValue(resultSet, columnMetadata);
            primitiveValues.put(columnMetadata.name(), primitiveValue);
        }
        return MElement.of(primitiveValues, timestamp);
    }

    public static MElement convert(
            final com.google.cloud.bigtable.data.v2.models.Row row,
            final Map<String, ColumnFamilyProperties> families,
            final Instant timestamp) {

        final Map<String, Object> primitiveValues = BigtableSchemaUtil.toPrimitiveValues(row, families);
        return MElement.of(primitiveValues, timestamp);
    }

    private static Schema.FieldType convertFieldType(final SqlType<?> type) {
        return switch (type.getCode()) {
            case BOOL -> Schema.FieldType.BOOLEAN;
            case STRING -> Schema.FieldType.STRING;
            case BYTES -> Schema.FieldType.BYTES;
            case INT64 -> Schema.FieldType.INT64;
            case FLOAT32 -> Schema.FieldType.FLOAT32;
            case FLOAT64 -> Schema.FieldType.FLOAT64;
            case DATE -> Schema.FieldType.DATE;
            case TIMESTAMP -> Schema.FieldType.TIMESTAMP;
            default -> Schema.FieldType.STRING;
        };
    }

    private static Object convertFieldValue(
            final ResultSet resultSet,
            final ColumnMetadata columnMetadata) {

        if(resultSet.isNull(columnMetadata.name())) {
            return null;
        }

        return switch (columnMetadata.type().getCode()) {
            case BOOL -> resultSet.getBoolean(columnMetadata.name());
            case STRING -> resultSet.getString(columnMetadata.name());
            case BYTES -> resultSet.getBytes(columnMetadata.name());
            case INT64 -> resultSet.getLong(columnMetadata.name());
            case FLOAT32 -> resultSet.getFloat(columnMetadata.name());
            case FLOAT64 -> resultSet.getDouble(columnMetadata.name());
            case DATE -> DateTimeUtil.toEpochDay(resultSet.getDate(columnMetadata.name()));
            case TIMESTAMP -> resultSet.getTimestamp(columnMetadata.name()).toEpochMilli() * 1000L;
            default -> null;
        };
    }

    public static ModType getModType(final Entry entry) {
        return switch (entry) {
            case SetCell setCell -> ModType.SET_CELL;
            case DeleteFamily deleteFamily -> ModType.DELETE_FAMILY;
            case DeleteCells deleteCells -> ModType.DELETE_CELLS;
            default -> ModType.UNKNOWN;
        };
    }

    public static Schema createChangeRecordMutationSchemaA() {
        return Schema.builder()
                .withField("rowKey", Schema.FieldType.STRING)
                .withField("family", Schema.FieldType.STRING)
                .withField("qualifier", Schema.FieldType.STRING)
                .withField("value", Schema.FieldType.BYTES)
                .withField("timestamp", Schema.FieldType.TIMESTAMP)
                .build();
    }

    public static Schema createChangeRecordMutationSchema() {
        try (final InputStream is = FailureUtil.class.getResourceAsStream(RESOURCE_CDC_AVRO_SCHEMA_PATH)) {
            if(is == null) {
                //LOG.info("BadRecord avro file is not found: " + RESOURCE_CDC_AVRO_SCHEMA_PATH);
                try(final InputStream iss = Files.newInputStream(Path.of(RESOURCE_RUNTIME_CDC_AVRO_SCHEMA_PATH))) {
                    final String schemaJson = org.apache.commons.io.IOUtils.toString(iss,  StandardCharsets.UTF_8);
                    final org.apache.avro.Schema avroSchema = AvroSchemaUtil.convertSchema(schemaJson);
                    return Schema.of(avroSchema);
                } catch (Throwable e) {
                    throw new IllegalArgumentException("BadRecord avro file is not found", e);
                }
            }
            final String schemaJson = IOUtils.toString(is,  StandardCharsets.UTF_8);
            final org.apache.avro.Schema avroSchema = AvroSchemaUtil.convertSchema(schemaJson);
            return Schema.of(avroSchema);
        } catch (final IOException e) {
            throw new IllegalArgumentException("Not found event descriptor file", e);
        }
    }

    public static MElement convert(
            final ChangeStreamMutation mutation,
            final Instant timestamp) {

        ChangeStreamMutation.MutationType a = mutation.getType();

        final Map<String, Object> primitiveValues = new HashMap<>();
        primitiveValues.put("rowKey", mutation.getRowKey().asReadOnlyByteBuffer());
        primitiveValues.put("commitTimestamp", DateTimeUtil.toEpochMicroSecond(mutation.getCommitTime()));
        primitiveValues.put("tieBreaker", mutation.getTieBreaker());
        primitiveValues.put("sourceCluster", mutation.getSourceClusterId());
        primitiveValues.put("estimatedLowWatermarkTime", DateTimeUtil.toEpochMicroSecond(mutation.getEstimatedLowWatermarkTime()));

        final List<Map<String,Object>> entriesPrimitiveValues = new ArrayList<>();
        for(final Entry entry : mutation.getEntries()) {
            final Map<String, Object> entryPrimitiveValues = new HashMap<>();
            switch (entry) {
                case SetCell setCell -> {
                    entryPrimitiveValues.put("familyName", setCell.getFamilyName());
                    entryPrimitiveValues.put("qualifier", setCell.getQualifier().asReadOnlyByteBuffer());
                    entryPrimitiveValues.put("value", setCell.getValue().asReadOnlyByteBuffer());
                    entryPrimitiveValues.put("timestamp", setCell.getTimestamp());
                    entryPrimitiveValues.put("modType", ModType.SET_CELL.id);
                    entriesPrimitiveValues.add(entryPrimitiveValues);
                }
                case DeleteFamily deleteFamily -> {
                    entryPrimitiveValues.put("familyName", deleteFamily.getFamilyName());
                    entryPrimitiveValues.put("modType", ModType.DELETE_FAMILY.id);
                    entriesPrimitiveValues.add(entryPrimitiveValues);
                }
                case DeleteCells deleteCells -> {
                    entryPrimitiveValues.put("familyName", deleteCells.getFamilyName());
                    entryPrimitiveValues.put("qualifier", deleteCells.getQualifier().asReadOnlyByteBuffer());
                    entryPrimitiveValues.put("modType", ModType.DELETE_CELLS.id);
                    entriesPrimitiveValues.add(entryPrimitiveValues);
                }
                default -> {}
            }
        }

        primitiveValues.put("entries", entriesPrimitiveValues);

        return MElement.of(primitiveValues, timestamp);
    }

    public static Map<String, ColumnFamilyProperties> toMap(List<ColumnFamilyProperties> families) {
        final Map<String, ColumnFamilyProperties> map = new HashMap<>();
        if(families == null) {
            return map;
        }
        for(final ColumnFamilyProperties family : families) {
            map.put(family.family, family);
        }
        return map;
    }

    public static List<Mutation> toMutations(
            final List<ColumnFamilyProperties> families,
            final Map<String, Object> primitiveValues,
            final Map<String, Object> standardValues,
            final Instant timestamp) {

        final List<Mutation> mutations = new ArrayList<>();
        for(var family : families) {
            final List<Mutation> m = family.toMutation(primitiveValues, standardValues, timestamp);
            mutations.addAll(m);
        }
        return mutations;
    }

    public static Map<String, Object> toPrimitiveValues(
            final Row row,
            final Map<String, ColumnFamilyProperties> familyProperties) {

        final Map<String, Object> primitiveValues = new HashMap<>();
        for(final Family family : row.getFamiliesList()) {
            if(!familyProperties.containsKey(family.getName())) {
                continue;
            }
            final ColumnFamilyProperties familyProperty = familyProperties.get(family.getName());
            final Map<String, Object> values = familyProperty.toElement(family);
            primitiveValues.putAll(values);
        }
        return primitiveValues;
    }

    public static Map<String, Object> toPrimitiveValues(
            final com.google.cloud.bigtable.data.v2.models.Row row,
            final Map<String, ColumnFamilyProperties> families) {

        final Map<String, Object> primitiveValues = new HashMap<>();
        for(final Map.Entry<String, ColumnFamilyProperties> entry : families.entrySet()) {
            for(final ColumnQualifierProperties qualifier : entry.getValue().qualifiers) {
                final List<RowCell> cells = row.getCells(entry.getKey(), qualifier.name);
                final Object primitiveValue = qualifier.toPrimitiveValue(cells.getFirst().getValue());
                primitiveValues.put(qualifier.field, primitiveValue);
            }
        }

        /*
        for(final RowCell cell : row.getCells()) {
            if(!families.containsKey(cell.getFamily())) {
                continue;
            }
            final ColumnFamilyProperties family = families.get(cell.getFamily());
            for(final ColumnQualifierProperties qualifier : family.qualifiers) {
                if(!cell.getQualifier().toStringUtf8().equals(qualifier.name)) {
                    continue;
                }
                final Object primitiveValue = qualifier.toPrimitiveValue(cell.getValue());
                primitiveValues.put(qualifier.field, primitiveValue);
            }
        }

         */
        return primitiveValues;
    }

    public static KV<Long, Long> getRowMinMaxTimestamps(final Row row) {
        long max = 0;
        long min = Long.MAX_VALUE;
        for(final Family family : row.getFamiliesList()) {
            for(final Column column : family.getColumnsList()) {
                for(final Cell cell : column.getCellsList()) {
                    if(cell.getTimestampMicros() > max) {
                        max = cell.getTimestampMicros();
                    }
                    if(cell.getTimestampMicros() < min) {
                        min = cell.getTimestampMicros();
                    }
                }
            }
        }
        return KV.of(min, max);
    }

    public static class ColumnSetting implements Serializable {

        private String field;
        private String columnFamily;
        private String columnQualifier;
        private Boolean exclude;
        private Format format;
        private MutationOp mutationOp;

        public String getField() {
            return field;
        }

        public String getColumnFamily() {
            return columnFamily;
        }

        public String getColumnQualifier() {
            return columnQualifier;
        }

        public Boolean getExclude() {
            return exclude;
        }

        public Format getFormat() {
            return format;
        }

        public MutationOp getMutationOp() {
            return mutationOp;
        }

        public void setDefaults(final Format format, final String defaultColumnFamily, final MutationOp defaultMutationOp) {
            if (columnQualifier == null) {
                columnQualifier = field;
            }
            if (columnFamily == null) {
                columnFamily = defaultColumnFamily;
            }
            if (exclude == null) {
                exclude = false;
            }
            if (this.format == null) {
                this.format = format;
            }
            if (this.mutationOp == null) {
                this.mutationOp = defaultMutationOp;
            }
        }

        public List<String> validate() {
            final List<String> errorMessages = new ArrayList<>();
            if (field == null) {
                errorMessages.add("BigtableSink module's mappings parameter requires `field` parameter.");
            }
            return errorMessages;
        }
    }

    private static ByteString toByteString(final Format format, final Object primitiveValue) {
        return switch (format) {
            case text -> toByteStringText(primitiveValue);
            case bytes -> toByteStringBytes(primitiveValue);
            case hadoop -> toByteStringHadoop(primitiveValue);
            case avro -> {
                try {
                    final byte[] bytes = AvroSchemaUtil.encode(primitiveValue);
                    yield ByteString.copyFrom(bytes);
                } catch (IOException e) {
                    throw new RuntimeException("Failed to convert to avro ByteString", e);
                }
            }
            case avromap -> {
                final Map<String, Object> values = new HashMap<>();
                switch (primitiveValue) {
                    case Map<?,?> map -> values.putAll((Map<String, Object>)map);
                    case List<?> list -> {
                        for(final Object listValue : list) {
                            if(listValue == null) {
                                continue;
                            }
                            if(!(listValue instanceof Map)) {
                                throw new IllegalArgumentException("avro");
                            }
                            final Map<?,?> map = (Map<?,?>) listValue;
                            if(!map.containsKey("key") || !map.containsKey("value")) {
                                throw new IllegalArgumentException("avromap format requires fields key and value. but input: " + map);
                            }
                            String key = (String) map.get("key");
                            Object value = map.get("value");
                            values.put(key, value);
                        }
                    }
                    default -> throw new IllegalArgumentException("avromap is not supported to convert byte string");
                }
                try {
                    final byte[] bytes = AvroSchemaUtil.encode(values);
                    System.out.println("base64: " + Base64.getEncoder().encodeToString(bytes));
                    yield ByteString.copyFrom(bytes);
                } catch (IOException e) {
                    throw new RuntimeException("Failed to convert to avro ByteString", e);
                }
            }
            default -> throw new IllegalArgumentException("Not supported byte string convert format: " + format);
        };
    }

    public static ByteString toByteString(final Object primitiveValue) {
        if(primitiveValue == null) {
            return ByteString.copyFrom(new byte[0]);
        }
        final byte[] bytes = switch (primitiveValue) {
            case Boolean b -> Bytes.toBytes(b);
            case String s -> Bytes.toBytes(s);
            case byte[] bs -> bs;
            case ByteBuffer bb -> bb.array();
            case ByteString bs -> bs.toByteArray();
            case ByteArray ba -> ba.toByteArray();
            case BigDecimal bd -> Bytes.toBytes(bd);
            case Short s -> Bytes.toBytes(s);
            case Integer i -> Bytes.toBytes(i);
            case Long l -> Bytes.toBytes(l);
            case Float f -> Bytes.toBytes(f);
            case Double d -> Bytes.toBytes(d);
            default -> throw new IllegalArgumentException("Not supported bytes class: " + primitiveValue.getClass());
        };
        return ByteString.copyFrom(bytes);
    }

    public static ByteString toByteStringText(final Object primitiveValue) {
        if(primitiveValue == null) {
            return ByteString.copyFrom(new byte[0]);
        }
        final String text = switch (primitiveValue) {
            case String s -> s;
            case Utf8 u -> u.toString();
            case byte[] bs -> Base64.getEncoder().encodeToString(bs);
            case ByteBuffer bb -> Base64.getEncoder().encodeToString(bb.array());
            case ByteString bs -> Base64.getEncoder().encodeToString(bs.toByteArray());
            case ByteArray ba -> Base64.getEncoder().encodeToString(ba.toByteArray());
            default -> primitiveValue.toString();
        };
        return ByteString.copyFrom(text, StandardCharsets.UTF_8);
    }

    public static ByteString toByteStringBytes(final Object primitiveValue) {
        if(primitiveValue == null) {
            return ByteString.copyFrom(new byte[0]);
        }
        final byte[] bytes = switch (primitiveValue) {
            case Boolean b -> Bytes.toBytes(b);
            case String s -> Bytes.toBytes(s);
            case Utf8 u -> Bytes.toBytes(u.toString());
            case byte[] bs -> bs;
            case ByteBuffer bb -> bb.array();
            case ByteString bs -> bs.toByteArray();
            case ByteArray ba -> ba.toByteArray();
            case BigDecimal bd -> Bytes.toBytes(bd);
            case Short s -> Bytes.toBytes(s);
            case Integer i -> Bytes.toBytes(i);
            case Long l -> Bytes.toBytes(l);
            case Float f -> Bytes.toBytes(f);
            case Double d -> Bytes.toBytes(d);
            default -> throw new IllegalArgumentException("Not supported bytes class: " + primitiveValue.getClass());
        };
        return ByteString.copyFrom(bytes);
    }

    public static ByteString toByteStringHadoop(final Object primitiveValue) {
        final Writable writable = toWritable(primitiveValue);
        final byte[] bytes = WritableUtils.toByteArray(writable);
        return ByteString.copyFrom(bytes);
    }

    public static Object toPrimitiveValue(final Format format, final Schema.FieldType fieldtype, final ByteString byteString) {
        return switch (format) {
            case bytes -> toPrimitiveValueFromBytes(fieldtype, byteString);
            case text -> ElementSchemaUtil.getAsPrimitive(fieldtype, new String(byteString.toByteArray(), StandardCharsets.UTF_8));
            case avro, avromap -> {
                try {
                    yield AvroSchemaUtil.decode(fieldtype, byteString.toByteArray());
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
            case hadoop -> toPrimitiveValueFromWritable(fieldtype, byteString);
        };
    }

    public static Object toPrimitiveValueFromBytes(final Schema.FieldType fieldtype, final ByteString byteString) {
        if (byteString == null) {
            return null;
        }
        final byte[] bytes = byteString.toByteArray();
        return switch (fieldtype.getType()) {
            case bool -> Bytes.toBoolean(bytes);
            case string, json -> Bytes.toString(bytes);
            case bytes -> ByteBuffer.wrap(bytes);
            case int16 -> Bytes.toShort(bytes);
            case int32, date, enumeration -> Bytes.toInt(bytes);
            case int64, time, timestamp -> Bytes.toLong(bytes);
            case float32 -> Bytes.toFloat(bytes);
            case float64 -> Bytes.toDouble(bytes);
            default -> throw new IllegalArgumentException("Not supported deserialize type: " + fieldtype.getType());
        };
    }

    public static Object toPrimitiveValueFromWritable(Schema.FieldType fieldtype, final ByteString byteString) {
        if(byteString == null) {
            return null;
        }
        return toPrimitiveValueFromWritable(fieldtype, byteString.toByteArray());
    }

    public static Object toPrimitiveValueFromWritable(Schema.FieldType fieldType, final byte[] bytes) {
        if(fieldType == null || bytes == null) {
            return null;
        }
        final Writable writable = getWritable(fieldType);

        try(final ByteArrayInputStream is = new ByteArrayInputStream(bytes);
            final DataInputStream ds = new DataInputStream(is)) {
            writable.readFields(ds);
            return toPrimitiveValue(writable);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static Object toPrimitiveValue(Writable writable) {
        return switch (writable) {
            case BooleanWritable b -> b.get();
            case Text t -> t.toString();
            case BytesWritable b -> b.getBytes();
            case ShortWritable s -> s.get();
            case VIntWritable i -> i.get();
            case VLongWritable l -> l.get();
            case FloatWritable f -> f.get();
            case DoubleWritable d -> d.get();
            case ArrayWritable arrayWritable -> {
                final List<Object> list = new ArrayList<>();
                for(Writable w : arrayWritable.get()) {
                    final Object o = toPrimitiveValue(w);
                    list.add(o);
                }
                yield list;
            }
            case MapWritable m -> {
                final Map<String, Object> map = new HashMap<>();
                for(final Map.Entry<Writable,Writable> entry : m.entrySet()) {
                    final Object key = toPrimitiveValue(entry.getKey());
                    final Object value = toPrimitiveValue(entry.getValue());
                    map.put(key.toString(), value);
                }
                yield map;
            }
            default -> throw new IllegalArgumentException();
        };
    }

    private static Value toValue(final Object primitiveValue) {
        return switch (primitiveValue) {
            case Boolean b -> Value.newBuilder().setBoolValue(b).build();
            case String s -> Value.newBuilder().setStringValue(s).build();
            case Integer i -> Value.newBuilder().setIntValue(i).build();
            case Long l -> Value.newBuilder().setIntValue(l).build();
            case Float f -> Value.newBuilder().setFloatValue(f).build();
            case Double d -> Value.newBuilder().setFloatValue(d).build();
            case ByteBuffer bb -> Value.newBuilder().setBytesValue(ByteString.copyFrom(bb)).build();
            case byte[] b -> Value.newBuilder().setBytesValue(ByteString.copyFrom(b)).build();
            default -> throw new IllegalArgumentException();
        };
    }

    private static Writable getWritable(final Schema.FieldType fieldType) {
        return switch (fieldType.getType()) {
            case bool -> new BooleanWritable();
            case int16 -> new ShortWritable();
            case int32, date -> new VIntWritable();
            case int64, time, timestamp -> new VLongWritable();
            case float32 -> new FloatWritable();
            case float64 -> new DoubleWritable();
            case string, json -> new Text();
            case bytes -> new BytesWritable();
            case map, element -> new MapWritable();
            case array -> {
                final Class<? extends Writable> writableClass = getWritableClass(fieldType.getArrayValueType());
                yield new ArrayWritable(writableClass);
            }
            default -> throw new IllegalArgumentException();
        };
    }

    private static Class<? extends Writable> getWritableClass(final Schema.FieldType fieldType) {
        if(fieldType == null) {
            return NullWritable.class;
        }
        return switch (fieldType.getType()) {
            case bool -> BooleanWritable.class;
            case string, json -> Text.class;
            case bytes -> BytesWritable.class;
            case int16 -> ShortWritable.class;
            case int32, date, enumeration -> VIntWritable.class;
            case int64, time, timestamp -> VLongWritable.class;
            case float32 -> FloatWritable.class;
            case float64 -> DoubleWritable.class;
            case array -> switch (fieldType.getArrayValueType().getType()) {
                case bool -> BoolArrayWritable.class;
                case string, json -> TextArrayWritable.class;
                case bytes -> BytesArrayWritable.class;
                case int16 -> ShortArrayWritable.class;
                case int32, date, enumeration -> IntArrayWritable.class;
                case int64, time, timestamp -> LongArrayWritable.class;
                case float32 -> FloatArrayWritable.class;
                case float64 -> DoubleArrayWritable.class;
                default -> throw new IllegalArgumentException();
            };
            case map, element -> MapWritable.class;
            default -> throw new IllegalArgumentException();
        };
    }

    private static Writable toWritable(final Object value) {
        if(value == null) {
            return NullWritable.get();
        }
        return switch (value) {
            case Boolean b -> new BooleanWritable(b);
            case String s -> new Text(s);
            case byte[] bs -> new BytesWritable(bs);
            case ByteBuffer bb -> new BytesWritable(bb.array());
            case ByteString bs -> new BytesWritable(bs.toByteArray());
            case ByteArray ba -> new BytesWritable(ba.toByteArray());
            case BigDecimal bd -> new BytesWritable(bd.toBigInteger().toByteArray());
            case Short s -> new ShortWritable(s);
            case Integer i -> new VIntWritable(i);
            case Long l -> new VLongWritable(l);
            case Float f -> new FloatWritable(f);
            case Double d -> new DoubleWritable(d);
            case Collection<?> c -> {
                if(c.isEmpty()) {
                    yield new TextArrayWritable(new Writable[0]);
                }
                Object v = null;
                final Writable[] array = new Writable[c.size()];
                int i=0;
                for(final Object o : c) {
                    array[i] = toWritable(o);
                    i++;
                    v = o;
                }
                yield switch (v) {
                    case Boolean b -> new BoolArrayWritable(array);
                    case String s -> new TextArrayWritable(array);
                    case byte[] b -> new BytesArrayWritable(array);
                    case ByteBuffer bb -> new BytesArrayWritable(array);
                    case Short s -> new ShortArrayWritable(array);
                    case Integer ii -> new IntArrayWritable(array);
                    case Long l -> new LongArrayWritable(array);
                    case Float f -> new FloatArrayWritable(array);
                    case Double d -> new DoubleArrayWritable(array);
                    default -> throw new IllegalArgumentException();
                };
            }
            case Map<?, ?> m -> {
                final MapWritable mapWritable = new MapWritable();
                for(final Map.Entry<?, ?> entry : m.entrySet()) {
                    final Writable k = toWritable(entry.getKey());
                    final Writable v = toWritable(entry.getValue());
                    mapWritable.put(k, v);
                }
                yield mapWritable;
            }
            default -> throw new IllegalArgumentException();
        };
    }

    public static class BoolArrayWritable extends ArrayWritable {
        public BoolArrayWritable() {
            super(BooleanWritable.class);
        }
        public BoolArrayWritable(final Writable[] array) {
            super(BooleanWritable.class, array);
        }
    }

    public static class ShortArrayWritable extends ArrayWritable {
        public ShortArrayWritable() {
            super(ShortWritable.class);
        }
        public ShortArrayWritable(final Writable[] array) {
            super(ShortWritable.class, array);
        }
    }

    public static class IntArrayWritable extends ArrayWritable {
        public IntArrayWritable() {
            super(VIntWritable.class);
        }
        public IntArrayWritable(final Writable[] array) {
            super(VIntWritable.class, array);
        }
    }

    public static class LongArrayWritable extends ArrayWritable {
        public LongArrayWritable() {
            super(VLongWritable.class);
        }
        public LongArrayWritable(final Writable[] array) {
            super(VLongWritable.class);
        }
    }

    public static class FloatArrayWritable extends ArrayWritable {
        public FloatArrayWritable() {
            super(FloatWritable.class);
        }
        public FloatArrayWritable(final Writable[] array) {
            super(FloatWritable.class, array);
        }
    }

    public static class DoubleArrayWritable extends ArrayWritable {
        public DoubleArrayWritable() {
            super(DoubleWritable.class);
        }
        public DoubleArrayWritable(final Writable[] array) {
            super(DoubleWritable.class, array);
        }
    }

    public static class TextArrayWritable extends ArrayWritable {
        public TextArrayWritable() {
            super(Text.class);
        }
        public TextArrayWritable(final Writable[] array) {
            super(Text.class, array);
        }
    }

    public static class BytesArrayWritable extends ArrayWritable {
        public BytesArrayWritable() {
            super(BytesWritable.class);
        }

        public BytesArrayWritable(final Writable[] array) {
            super(BytesWritable.class, array);
        }
    }

}
