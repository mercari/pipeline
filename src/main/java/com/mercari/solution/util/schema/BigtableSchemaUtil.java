package com.mercari.solution.util.schema;

import com.google.bigtable.v2.*;
import com.google.cloud.ByteArray;
import com.google.cloud.bigtable.data.v2.models.RowCell;
import com.google.protobuf.ByteString;
import com.mercari.solution.module.Schema;
import com.mercari.solution.util.DateTimeUtil;
import com.mercari.solution.util.TemplateUtil;
import freemarker.template.Template;
import org.apache.avro.util.Utf8;
import org.apache.beam.sdk.values.KV;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.*;
import org.joda.time.Instant;

import java.io.*;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Collectors;

public class BigtableSchemaUtil {

    public enum Format {
        bytes,
        avro,
        hadoop,
        text
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

    public static class ColumnFamilyProperties implements Serializable {

        private String family;
        private List<ColumnQualifierProperties> qualifiers;
        private Format format;

        // for sink
        private MutationOp mutationOp;
        private TimestampType timestampType;

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

        public void setDefaults(
                final Format defaultFormat,
                final CellType cellType) {

            setDefaults(defaultFormat, null, null, cellType);
        }

        public void setDefaults(
                final Format defaultFormat,
                final MutationOp defaultOp,
                final TimestampType defaultTimestampType) {

            setDefaults(defaultFormat, defaultOp, defaultTimestampType, null);
        }

        private void setDefaults(
                final Format defaultFormat,
                final MutationOp defaultOp,
                final TimestampType defaultTimestampType,
                final CellType defaultCellType) {

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
            if(cellType == null) {
                cellType = Optional.ofNullable(defaultCellType).orElse(CellType.last);
            }
            if(qualifiers == null) {
                qualifiers = new ArrayList<>();
            }
            for(final ColumnQualifierProperties qualifier : qualifiers) {
                qualifier.setDefaults(format, mutationOp, timestampType);
            }
        }

        public List<String> extractValueArgs() {
            final List<String> valueArgs = new ArrayList<>();
            for(final ColumnQualifierProperties qualifier : qualifiers) {
                valueArgs.add(qualifier.field);
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
                Mutation mutation = Mutation.newBuilder()
                        .setDeleteFromFamily(Mutation.DeleteFromFamily.newBuilder()
                                .setFamilyName(cf)
                                .build())
                        .build();
                mutations.add(mutation);
            } else {
                for(final ColumnQualifierProperties qualifier : qualifiers) {
                    final Mutation mutation = qualifier.toMutation(cf, primitiveValues, standardValues, timestamp);
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


        public List<String> validate(int i, int j) {
            final List<String> errorMessages = new ArrayList<>();
            if(name == null) {
                errorMessages.add("parameters.columns[" + i + "].qualifiers[" + j + "].name must not be null");
            }
            if(field == null) {
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

        public void setDefaults(
                final Format defaultFormat,
                final MutationOp defaultOp,
                final TimestampType defaultTimestampType) {

            setDefaults(defaultFormat, defaultOp, defaultTimestampType, null);
        }

        public void setDefaults(
                final Format defaultFormat,
                final MutationOp defaultOp,
                final TimestampType defaultTimestampType,
                final CellType defaultCellType) {

            if(format == null) {
                format = defaultFormat;
            }
            if(mutationOp == null) {
                mutationOp = defaultOp;
            }
            if(timestampType == null) {
                timestampType = defaultTimestampType;
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

            final String cq = TemplateUtil.executeStrictTemplate(templateQualifier, standardValues);
            return switch (mutationOp) {
                case SET_CELL -> {
                    final ByteString fieldValue = toByteString(format, primitiveValues.get(field));
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
                            .setInput(toValue(primitiveValues.get(field)))
                            .setTimestamp(Value.newBuilder().setTimestampValue(DateTimeUtil.toProtoTimestamp(timestampMicros)))
                            .build();
                    yield Mutation.newBuilder().setAddToCell(cell).build();
                }
                case MERGE_TO_CELL -> {
                    final Mutation.MergeToCell cell = Mutation.MergeToCell.newBuilder()
                            .setFamilyName(cf)
                            .setColumnQualifier(Value.newBuilder().setBytesValue(ByteString.copyFrom(cq, StandardCharsets.UTF_8)))
                            .setInput(toValue(primitiveValues.get(field)))
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
            case avro -> {
                try {
                    yield AvroSchemaUtil.decode(fieldtype, byteString.toByteArray());
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
            case hadoop -> toPrimitiveValueFromWritable(fieldtype, byteString);
            case text -> ElementSchemaUtil.getAsPrimitive(fieldtype, new String(byteString.toByteArray(), StandardCharsets.UTF_8));
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
