package com.mercari.solution.util.schema;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonPrimitive;
import com.google.protobuf.ByteString;
import com.mercari.solution.util.DateTimeUtil;
import com.mercari.solution.util.schema.converter.JsonToRowConverter;
import com.mercari.solution.util.schema.converter.RowToJsonConverter;
import org.apache.beam.sdk.extensions.sql.impl.utils.CalciteUtils;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.logicaltypes.EnumerationType;
import org.apache.beam.sdk.schemas.logicaltypes.SqlTypes;
import org.apache.beam.sdk.values.Row;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Instant;
import org.joda.time.ReadableDateTime;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class RowSchemaUtil {

    public static final String OPTION_NAME_DEFAULT_VALUE = "default";

    private static final Schema.FieldType REQUIRED_LOGICAL_DATETIME = Schema.FieldType.logicalType(SqlTypes.DATETIME).withNullable(false);
    private static final Schema.FieldType NULLABLE_LOGICAL_DATETIME = Schema.FieldType.logicalType(SqlTypes.DATETIME).withNullable(true);

    public static Schema.Builder toBuilder(final Schema schema) {
        return toBuilder(schema, null, false);
    }

    public static Schema.Builder toBuilder(final Schema schema, final List<String> filter) {
        return toBuilder(schema, filter, false);
    }

    public static Schema.Builder toBuilder(final Schema schema, final List<String> filter, final boolean exclude) {
        final Schema.Builder builder = Schema.builder();
        for(final Schema.Field field : schema.getFields()) {
            if(exclude) {
                if(filter == null || !filter.contains(field.getName())) {
                    builder.addField(field);
                }
            } else {
                if(filter == null || filter.contains(field.getName())) {
                    builder.addField(field);
                }
            }
        }
        return builder;
    }

    public static Row.FieldValueBuilder toBuilder(final Row row) {
        return toBuilder(row.getSchema(), row);
    }

    public static Row.FieldValueBuilder toBuilder(final Schema schema, final Row row) {
        return toBuilder(schema, row, new HashMap<>());
    }

    public static Row.FieldValueBuilder toBuilder(final Schema schema, final Row row, final Map<String, String> renameFields) {
        final Row.FieldValueBuilder builder = Row.withSchema(schema).withFieldValues(new HashMap<>());
        for(final Schema.Field field : schema.getFields()) {
            final String getFieldName = renameFields.getOrDefault(field.getName(), field.getName());
            final String setFieldName = field.getName();
            if(row.getSchema().hasField(getFieldName)) {
                if(row.getValue(getFieldName) == null) {
                    builder.withFieldValue(setFieldName, null);
                    continue;
                }
                final Schema.Field rowField = row.getSchema().getField(getFieldName);
                if(!field.getType().getTypeName().equals(rowField.getType().getTypeName())) {
                    builder.withFieldValue(setFieldName, null);
                    continue;
                }

                switch (field.getType().getTypeName()) {
                    case ITERABLE:
                    case ARRAY: {
                        if(field.getType().getCollectionElementType().getTypeName().equals(Schema.TypeName.ROW)) {
                            final List<Row> children = new ArrayList<>();
                            for(final Row child : row.<Row>getArray(getFieldName)) {
                                if(child == null) {
                                    children.add(null);
                                } else {
                                    children.add(toBuilder(field.getType().getCollectionElementType().getRowSchema(), child).build());
                                }
                            }
                            builder.withFieldValue(setFieldName, children);
                        } else {
                            builder.withFieldValue(setFieldName, row.getValue(getFieldName));
                        }
                        break;
                    }
                    case ROW: {
                        final Row child = toBuilder(field.getType().getRowSchema(), row.getRow(getFieldName)).build();
                        builder.withFieldValue(setFieldName, child);
                        break;
                    }
                    default:
                        builder.withFieldValue(setFieldName, row.getValue(getFieldName));
                        break;
                }
            } else if(renameFields.containsValue(setFieldName)) {
                final String getOuterFieldName = renameFields.entrySet().stream()
                        .filter(e -> e.getValue().equals(setFieldName))
                        .map(Map.Entry::getKey)
                        .findAny()
                        .orElse(setFieldName);
                if(row.getValue(getOuterFieldName) == null) {
                    builder.withFieldValue(setFieldName, null);
                    continue;
                }
                final Schema.Field rowField = row.getSchema().getField(getOuterFieldName);
                if(!field.getType().getTypeName().equals(rowField.getType().getTypeName())) {
                    builder.withFieldValue(setFieldName, null);
                    continue;
                }

                switch (field.getType().getTypeName()) {
                    case ITERABLE:
                    case ARRAY: {
                        if(field.getType().getCollectionElementType().getTypeName().equals(Schema.TypeName.ROW)) {
                            final List<Row> children = new ArrayList<>();
                            for(final Row child : row.<Row>getArray(getOuterFieldName)) {
                                if(child == null) {
                                    children.add(null);
                                } else {
                                    children.add(toBuilder(field.getType().getCollectionElementType().getRowSchema(), child).build());
                                }
                            }
                            builder.withFieldValue(setFieldName, children);
                        } else {
                            builder.withFieldValue(setFieldName, row.getValue(getOuterFieldName));
                        }
                        break;
                    }
                    case ROW: {
                        final Row child = toBuilder(field.getType().getRowSchema(), row.getRow(getOuterFieldName)).build();
                        builder.withFieldValue(setFieldName, child);
                        break;
                    }
                    default:
                        builder.withFieldValue(setFieldName, row.getValue(getOuterFieldName));
                        break;
                }
            } else {
                builder.withFieldValue(setFieldName, null);
            }
        }
        return builder;
    }

    public static Schema merge(final Schema base, final Schema addition) {
        Schema.Builder builder = Schema.builder();
        for(final Schema.Field field : base.getFields()) {
            builder.addField(field);
        }
        for(final Schema.Field field : addition.getFields()) {
            if(base.hasField(field.getName())) {
                continue;
            }
            builder.addField(field);
        }
        return builder.build();
    }

    public static Schema addSchema(final Schema schema, final List<Schema.Field> fields) {
        Schema.Builder builder = Schema.builder();
        for(final Schema.Field field : schema.getFields()) {
            builder.addField(field);
        }
        builder.addFields(fields);
        return builder.build();
    }

    public static Schema removeFields(final Schema schema, final Collection<String> excludeFields) {
        if(excludeFields == null || excludeFields.size() == 0) {
            return schema;
        }

        final Schema.Builder builder = Schema.builder();
        for(final Schema.Field field : schema.getFields()) {
            if(excludeFields.contains(field.getName())) {
                continue;
            }
            builder.addField(field);
        }

        final Schema.Options.Builder optionBuilder = Schema.Options.builder();
        for(final String optionName : schema.getOptions().getOptionNames()) {
            optionBuilder.setOption(optionName, schema.getOptions().getType(optionName), schema.getOptions().getValue(optionName));
        }
        builder.setOptions(optionBuilder);

        return builder.build();
    }

    public static Schema renameFields(final Schema schema, final Map<String, String> renameFields) {
        Schema.Builder builder = Schema.builder();
        for(final Schema.Field field : schema.getFields()) {
            if(renameFields.containsKey(field.getName())) {
                Schema.Field renameField = Schema.Field.of(renameFields.get(field.getName()), field.getType())
                        .withOptions(field.getOptions())
                        .withDescription(field.getDescription());
                builder.addField(renameField);
            } else {
                builder.addField(field);
            }
        }
        return builder.build();
    }

    public static Row merge(final Row row, final Map<String, ? extends Object> values) {
        return merge(row.getSchema(), row, values);
    }

    public static Row merge(final Schema schema, final Row row, final Map<String, ? extends Object> values) {
        final Row.FieldValueBuilder builder = Row.withSchema(schema).withFieldValues(new HashMap<>());
        for(Schema.Field field : schema.getFields()) {
            if(values.containsKey(field.getName())) {
                builder.withFieldValue(field.getName(), values.get(field.getName()));
            } else if(row.getSchema().hasField(field.getName())) {
                builder.withFieldValue(field.getName(), row.getValue(field.getName()));
            } else {
                builder.withFieldValue(field.getName(), null);
            }
        }
        return builder.build();
    }

    public static Row create(final Schema schema, Map<String, Object> values) {
        final Map<String, Object> v = new HashMap<>();
        for(final Schema.Field field : schema.getFields()) {
            v.put(field.getName(), values.getOrDefault(field.getName(), null));
        }
        return Row
                .withSchema(schema)
                .withFieldValues(v)
                .build();
    }

    public static Schema selectFields(Schema schema, final List<String> fields) {
        return selectFieldsBuilder(schema, fields).build();
    }

    public static Schema.Builder selectFieldsBuilder(Schema schema, final List<String> fields) {
        final Schema.Builder builder = Schema.builder();
        final Map<String, List<String>> childFields = new HashMap<>();
        for(String field : fields) {
            if(field.contains(".")) {
                final String[] strs = field.split("\\.", 2);
                if(childFields.containsKey(strs[0])) {
                    childFields.get(strs[0]).add(strs[1]);
                } else {
                    childFields.put(strs[0], new ArrayList<>(Arrays.asList(strs[1])));
                }
            } else {
                builder.addField(schema.getField(field));
            }
        }

        if(!childFields.isEmpty()) {
            for(var entry : childFields.entrySet()) {
                final Schema.Field childField = schema.getField(entry.getKey());
                switch (childField.getType().getTypeName()) {
                    case ROW: {
                        final Schema childSchema = selectFields(childField.getType().getRowSchema(), entry.getValue());
                        builder.addField(entry.getKey(), Schema.FieldType.row(childSchema));
                        break;
                    }
                    case ITERABLE:
                    case ARRAY: {
                        if(!childField.getType().getCollectionElementType().getTypeName().equals(Schema.TypeName.ROW)) {
                            throw new IllegalStateException();
                        }
                        final Schema childSchema = selectFields(childField.getType().getCollectionElementType().getRowSchema(), entry.getValue());
                        builder.addField(entry.getKey(), Schema.FieldType.array(Schema.FieldType.row(childSchema).withNullable(true)).withNullable(true));
                        break;
                    }
                    default:
                        throw new IllegalStateException();
                }
            }
        }
        return builder;
    }

    public static Schema flatten(final Schema schema, final String path, final boolean addPrefix) {
        final List<String> paths = Arrays.asList(path.split("\\."));
        final List<Schema.Field> fields = flattenFields(schema, paths, null, addPrefix);
        return Schema.builder().addFields(fields).build();
    }

    public static List<Row> flatten(final Schema schema, final Row row, final String path, final boolean addPrefix) {
        final List<String> paths = Arrays.asList(path.split("\\."));
        final List<Object> values = flattenValues(row, paths, null, addPrefix);
        return values.stream()
                .map(v -> Row.withSchema(schema).withFieldValues((Map<String, Object>)v).build())
                .collect(Collectors.toList());
    }

    private static List<Schema.Field> flattenFields(final Schema schema, final List<String> paths, final String prefix, final boolean addPrefix) {
        return schema.getFields().stream()
                .flatMap(f -> {
                    final String name;
                    if(addPrefix) {
                        name = (prefix == null ? "" : prefix + "_") + f.getName();
                    } else {
                        name = f.getName();
                    }

                    if(paths.isEmpty() || !f.getName().equals(paths.get(0))) {
                        return Stream.of(Schema.Field.of(name, f.getType()).withNullable(true));
                    }

                    if(Schema.TypeName.ARRAY.equals(f.getType().getTypeName())) {
                        final Schema.FieldType elementType = f.getType().getCollectionElementType();
                        if(Schema.TypeName.ROW.equals(elementType.getTypeName())) {
                            return flattenFields(
                                    elementType.getRowSchema(),
                                    paths.subList(1, paths.size()), name, addPrefix)
                                    .stream();
                        } else {
                            return Stream.of(Schema.Field.of(f.getName(), elementType).withNullable(true));
                        }
                    } else {
                        return Stream.of(Schema.Field.of(name + f.getName(), f.getType()).withNullable(true));
                    }
                })
                .collect(Collectors.toList());
    }

    private static List<Object> flattenValues(final Row row, final List<String> paths, final String prefix, final boolean addPrefix) {
        final Map<String, Object> values = new HashMap<>();
        Schema.Field pathField = null;
        String pathName = null;
        for(final Schema.Field field : row.getSchema().getFields()) {
            final String name;
            if(addPrefix) {
                name = (prefix == null ? "" : prefix + "_") + field.getName();
            } else {
                name = field.getName();
            }
            if(paths.isEmpty() || !field.getName().equals(paths.get(0))) {
                values.put(name, row.getValue(field.getName()));
            } else {
                pathName = name;
                pathField = field;
            }
        }

        if(pathField == null) {
            return Arrays.asList(values);
        }

        if(row.getValue(pathField.getName()) == null) {
            return Arrays.asList(values);
        }

        if(Schema.TypeName.ARRAY.equals(pathField.getType().getTypeName())) {
            final List<Object> arrayValues = new ArrayList<>();
            final Collection<Object> array = row.getArray(pathField.getName());
            for(final Object value : array) {
                if(Schema.TypeName.ROW.equals(pathField.getType().getCollectionElementType().getTypeName())) {
                    final List<Object> list = flattenValues(
                            (Row)value,
                            paths.subList(1, paths.size()), pathName, addPrefix);
                    for(Object obj : list) {
                        if(obj instanceof Map) {
                            ((Map<String, Object>)obj).putAll(values);
                        }
                        arrayValues.add(obj);
                    }
                } else {
                    arrayValues.add(value);
                }
            }
            return arrayValues;
        } else {
            values.put(pathName, row.getValue(pathField.getName()));
        }

        return Arrays.asList(values);
    }

    public static boolean isLogicalTypeDate(final Schema.FieldType fieldType) {
        if(fieldType.getLogicalType() == null) {
            return false;
        }
        return CalciteUtils.DATE.typesEqual(fieldType) ||
                CalciteUtils.NULLABLE_DATE.typesEqual(fieldType) ||
                fieldType.getLogicalType().getIdentifier().equals(CalciteUtils.DATE.getLogicalType().getIdentifier()) ||
                fieldType.getLogicalType().getIdentifier().equals(CalciteUtils.NULLABLE_DATE.getLogicalType().getIdentifier());
    }

    public static boolean isLogicalTypeTime(final Schema.FieldType fieldType) {
        if(fieldType.getLogicalType() == null) {
            return false;
        }
        return CalciteUtils.TIME.typesEqual(fieldType) ||
                CalciteUtils.NULLABLE_TIME.typesEqual(fieldType) ||
                fieldType.getLogicalType().getIdentifier().equals(CalciteUtils.TIME.getLogicalType().getIdentifier()) ||
                fieldType.getLogicalType().getIdentifier().equals(CalciteUtils.NULLABLE_TIME.getLogicalType().getIdentifier());
    }

    public static boolean isLogicalTypeEnum(final Schema.FieldType fieldType) {
        if(fieldType.getLogicalType() == null) {
            return false;
        }
        return fieldType.getLogicalType().getIdentifier().equals(EnumerationType.IDENTIFIER);
    }

    public static boolean isLogicalTypeTimestamp(final Schema.FieldType fieldType) {
        if(fieldType.getLogicalType() == null) {
            return false;
        }
        return CalciteUtils.TIMESTAMP.typesEqual(fieldType) || CalciteUtils.NULLABLE_TIMESTAMP.typesEqual(fieldType);
    }

    public static boolean isLogicalTypeDateTime(final Schema.FieldType fieldType) {
        if(fieldType.getLogicalType() == null) {
            return false;
        }
        return REQUIRED_LOGICAL_DATETIME.typesEqual(fieldType) || NULLABLE_LOGICAL_DATETIME.typesEqual(fieldType);
    }

    public static boolean isSqlTypeJson(final Schema.Options fieldOptions) {
        if(fieldOptions == null) {
            return false;
        }
        if(!fieldOptions.hasOption("sqlType")) {
            return false;
        }
        final String sqlType = fieldOptions.getValue("sqlType", String.class);
        return "json".equalsIgnoreCase(sqlType);
    }

    public static Object getRawValue(final Row row, final String fieldName) {
        if(row == null) {
            return null;
        }
        if(!row.getSchema().hasField(fieldName)) {
            return null;
        }
        if(row.getValue(fieldName) == null) {
            return null;
        }
        return row.getValue(fieldName);
    }

    public static Object getValue(final Row row, final String fieldName) {
        if(row == null) {
            return null;
        }
        if(!row.getSchema().hasField(fieldName)) {
            return null;
        }
        if(row.getValue(fieldName) == null) {
            return null;
        }
        final Schema.Field field = row.getSchema().getField(fieldName);
        switch (field.getType().getTypeName()) {
            case BOOLEAN:
                return row.getBoolean(fieldName);
            case STRING:
                return row.getString(fieldName);
            case BYTES:
                return row.getBytes(fieldName);
            case BYTE:
                return row.getByte(fieldName);
            case INT16:
                return row.getInt16(fieldName);
            case INT32:
                return row.getInt32(fieldName);
            case INT64:
                return row.getInt64(fieldName);
            case FLOAT:
                return row.getFloat(fieldName);
            case DOUBLE:
                return row.getDouble(fieldName);
            case DECIMAL:
                return row.getDecimal(fieldName);
            case DATETIME:
                return row.getDateTime(fieldName).toInstant();
            case LOGICAL_TYPE:
                if(isLogicalTypeDate(field.getType())) {
                    return row.getValue(fieldName); //LocalDate
                } else if(isLogicalTypeTime(field.getType())) {
                    return row.getValue(fieldName); //LocalTime
                } else if(isLogicalTypeEnum(field.getType())) {
                    final EnumerationType.Value enumValue = row.getValue(fieldName);
                    return ((EnumerationType)field.getType().getLogicalType()).getValues().get(enumValue.getValue());
                }
                return row.getValue(fieldName);
            case ARRAY:
            case ITERABLE:
                if(field.getType().getCollectionElementType().getTypeName().equals(Schema.TypeName.DATETIME)) {
                    return row.getArray(fieldName).stream()
                            .map(v -> {
                                if(v == null) {
                                    return null;
                                }
                                final Schema.FieldType arrayType = field.getType().getCollectionElementType();
                                switch (arrayType.getTypeName()) {
                                    case DATETIME:
                                        return ((ReadableDateTime)v).toInstant();
                                    case LOGICAL_TYPE:
                                        if(isLogicalTypeEnum(field.getType().getCollectionElementType())) {
                                            final EnumerationType.Value ev = (EnumerationType.Value)v;
                                            return ((EnumerationType)arrayType.getLogicalType()).getValues().get(ev.getValue());
                                        }
                                    default:
                                        return v;
                                }
                            })
                            .collect(Collectors.toList());
                } else {
                    return row.getArray(fieldName);
                }
            case ROW:
                return row.getRow(fieldName);
            case MAP:
                return row.getMap(fieldName);
            default:
                return null;
        }
    }

    public static String getAsString(final Row row, final String field) {
        if(row == null) {
            return null;
        }
        if(!row.getSchema().hasField(field)) {
            return null;
        }
        if(row.getValue(field) == null) {
            return null;
        }
        return row.getValue(field).toString();
    }

    public static String getAsString(final Object row, final String field) {
        if(row == null) {
            return null;
        }
        return getAsString((Row) row, field);
    }

    public static Long getAsLong(final Row row, final String fieldName) {
        if(row == null) {
            return null;
        }
        if(!row.getSchema().hasField(fieldName)) {
            return null;
        }
        if(row.getValue(fieldName) == null) {
            return null;
        }
        final Schema.Field field = row.getSchema().getField(fieldName);
        return switch (field.getType().getTypeName()) {
            case BOOLEAN -> row.getBoolean(fieldName) ? 1L : 0L;
            case STRING -> {
                try {
                    yield Long.valueOf(row.getString(fieldName));
                } catch (Exception e) {
                    yield null;
                }
            }
            case BYTE -> row.getByte(fieldName).longValue();
            case INT16 -> row.getInt16(fieldName).longValue();
            case INT32 -> row.getInt32(fieldName).longValue();
            case INT64 -> row.getInt64(fieldName);
            case FLOAT -> row.getFloat(fieldName).longValue();
            case DOUBLE -> row.getDouble(fieldName).longValue();
            case DECIMAL -> row.getDecimal(fieldName).longValue();
            case LOGICAL_TYPE, DATETIME, BYTES, ARRAY, ITERABLE, ROW, MAP -> null;
            default -> null;
        };
    }

    public static Double getAsDouble(final Row row, final String fieldName) {
        if(row == null) {
            return null;
        }
        if(!row.getSchema().hasField(fieldName)) {
            return null;
        }
        if(row.getValue(fieldName) == null) {
            return null;
        }
        final Schema.Field field = row.getSchema().getField(fieldName);
        return switch (field.getType().getTypeName()) {
            case BOOLEAN -> row.getBoolean(fieldName) ? 1D : 0D;
            case STRING -> {
                try {
                    yield Double.valueOf(row.getString(fieldName));
                } catch (Exception e) {
                    yield null;
                }
            }
            case BYTE -> row.getByte(fieldName).doubleValue();
            case INT16 -> row.getInt16(fieldName).doubleValue();
            case INT32 -> row.getInt32(fieldName).doubleValue();
            case INT64 -> row.getInt64(fieldName).doubleValue();
            case FLOAT -> row.getFloat(fieldName).doubleValue();
            case DOUBLE -> row.getDouble(fieldName);
            case DECIMAL -> row.getDecimal(fieldName).doubleValue();
            case DATETIME -> Long.valueOf(row.getDateTime(fieldName).toInstant().getMillis()).doubleValue();
            case LOGICAL_TYPE, BYTES, ARRAY, ITERABLE, ROW, MAP -> null;
            default -> null;
        };
    }

    public static BigDecimal getAsBigDecimal(final Row row, final String fieldName) {
        if(row == null) {
            return null;
        }
        if(!row.getSchema().hasField(fieldName)) {
            return null;
        }
        if(row.getValue(fieldName) == null) {
            return null;
        }
        final Schema.Field field = row.getSchema().getField(fieldName);
        switch (field.getType().getTypeName()) {
            case BOOLEAN:
                return BigDecimal.valueOf(row.getBoolean(fieldName) ? 1D : 0D);
            case STRING: {
                try {
                    return BigDecimal.valueOf(Double.valueOf(row.getString(fieldName)));
                } catch (Exception e) {
                    return null;
                }
            }
            case BYTE:
                return BigDecimal.valueOf(row.getByte(fieldName).longValue());
            case INT16:
                return BigDecimal.valueOf(row.getInt16(fieldName).longValue());
            case INT32:
                return BigDecimal.valueOf(row.getInt32(fieldName).longValue());
            case INT64:
                return BigDecimal.valueOf(row.getInt64(fieldName));
            case FLOAT:
                return BigDecimal.valueOf(row.getFloat(fieldName).doubleValue());
            case DOUBLE:
                return BigDecimal.valueOf(row.getDouble(fieldName));
            case DECIMAL:
                return row.getDecimal(fieldName);
            case LOGICAL_TYPE:
            case DATETIME:
            case BYTES:
            case ARRAY:
            case ITERABLE:
            case ROW:
            case MAP:
            default:
                return null;
        }
    }

    // for bigtable
    public static ByteString getAsByteString(final Row row, final String fieldName) {
        final Object primitiveValue = getAsPrimitive(row, fieldName);
        return BigtableSchemaUtil.toByteString(primitiveValue);
    }

    public static byte[] getBytes(final Row row, final String fieldName) {
        if(row == null) {
            return null;
        }
        if(!row.getSchema().hasField(fieldName)) {
            return null;
        }
        if(row.getValue(fieldName) == null) {
            return null;
        }
        switch (row.getSchema().getField(fieldName).getType().getTypeName()) {
            case STRING:
                return Base64.getDecoder().decode(row.getString(fieldName));
            case BYTES:
                return row.getBytes(fieldName);
            default:
                return null;
        }
    }

    public static ByteBuffer getAsBytes(final Row row, final String fieldName) {
        if(row == null) {
            return null;
        }
        if(!row.getSchema().hasField(fieldName)) {
            return null;
        }
        final Object value = row.getValue(fieldName);
        if(value == null) {
            return null;
        }
        final byte[] bytes = switch (row.getSchema().getField(fieldName).getType().getTypeName()) {
            case BYTES -> (byte[])value;
            case STRING -> Base64.getDecoder().decode((String)value);
            default -> null;
        };

        return Optional
                .ofNullable(bytes)
                .map(ByteBuffer::wrap)
                .orElse(null);
    }

    public static Instant getAsInstant(final Row row, final String fieldName) {
        return getTimestamp(row, fieldName, null);
    }

    public static Instant getTimestamp(final Row row, final String fieldName, final Instant defaultTimestamp) {
        final Schema.Field field = row.getSchema().getField(fieldName);
        if(field == null) {
            return defaultTimestamp;
        }
        if(!row.getSchema().hasField(fieldName)) {
            return defaultTimestamp;
        }
        final Object value = row.getValue(fieldName);
        if(value == null) {
            return defaultTimestamp;
        }
        switch (field.getType().getTypeName()) {
            case DATETIME: {
                return (Instant) value;
            }
            case LOGICAL_TYPE: {
                if(RowSchemaUtil.isLogicalTypeDate(field.getType())) {
                    return (Instant) value;
                } else if(RowSchemaUtil.isLogicalTypeTimestamp(field.getType())) {
                    return (Instant) value;
                }
                return defaultTimestamp;
            }
            case STRING: {
                final String stringValue = value.toString();
                try {
                    return Instant.parse(stringValue);
                } catch (Exception e) {
                    return defaultTimestamp;
                }
            }
            case INT16: {
                return Instant.ofEpochMilli((short) value);
            }
            case INT32: {
                final LocalDate localDate = LocalDate.ofEpochDay((int) value);
                return new DateTime(localDate.getYear(), localDate.getMonthValue(), localDate.getDayOfMonth(),
                        0, 0, DateTimeZone.UTC).toInstant();
            }
            case INT64: {
                return Instant.ofEpochMilli((long) value);
            }
            case FLOAT: {
                return Instant.ofEpochMilli(((Float) value).longValue());
            }
            case DOUBLE: {
                return Instant.ofEpochMilli(((Double) value).longValue());
            }
            case BYTES:
            case BOOLEAN:
            case MAP:
            case DECIMAL:
            case BYTE:
            case ARRAY:
            case ITERABLE:
            case ROW:
            default:
                return defaultTimestamp;
        }
    }

    public static Object getAsPrimitive(final Row row, final String fieldName) {
        if(row == null || fieldName == null) {
            return null;
        }
        if(fieldName.contains(".")) {
            final String[] fields = fieldName.split("\\.", 2);
            final String parentField = fields[0];
            final Row child = row.getRow(parentField);
            return getAsPrimitive(child, fields[1]);
        }

        if(!row.getSchema().hasField(fieldName)) {
            return null;
        }
        final Schema.Field field = row.getSchema().getField(fieldName);
        final Object value = row.getValue(fieldName);
        return getAsPrimitive(field.getType(), value);
    }

    public static Object getAsPrimitive(final Object row, final Schema.FieldType fieldType, final String field) {
        if(field.contains(".")) {
            final String[] fields = field.split("\\.", 2);
            final String parentField = fields[0];
            final Object child = ((Row) row).getValue(parentField);
            return getAsPrimitive(child, fieldType, fields[1]);
        }

        final Object value = ((Row) row).getValue(field);
        if(value == null) {
            return null;
        }

        return getAsPrimitive(fieldType, value);
    }



    public static Object getAsPrimitive(final Schema.FieldType fieldType, final Object fieldValue) {
        if(fieldValue == null) {
            return null;
        }
        return switch (fieldType.getTypeName()) {
            case INT16 -> ((Short) fieldValue).intValue();
            case BYTES -> (byte[]) fieldValue;
            case INT32, INT64, FLOAT, DOUBLE, STRING, BOOLEAN -> fieldValue;
            case DECIMAL -> ((BigDecimal) fieldValue).doubleValue();
            case DATETIME -> {
                if(fieldValue instanceof Long) {
                    yield fieldValue;
                } else if(fieldValue instanceof Instant) {
                    yield ((Instant) fieldValue).getMillis() * 1000L;
                } else {
                    throw new IllegalStateException();
                }
            }
            case LOGICAL_TYPE -> {
                if (RowSchemaUtil.isLogicalTypeDate(fieldType)) {
                    if(fieldValue instanceof LocalDate) {
                        yield Long.valueOf(((LocalDate) fieldValue).toEpochDay()).intValue();
                    } else if(fieldValue instanceof Integer) {
                        yield fieldValue;
                    } else {
                        throw new IllegalStateException("Nut supported date type: " + fieldValue);
                    }
                } else if (RowSchemaUtil.isLogicalTypeTime(fieldType)) {
                    if(fieldValue instanceof LocalTime) {
                        yield ((LocalTime) fieldValue).toNanoOfDay() / 1000L;
                    } else if(fieldValue instanceof Long) {
                        yield fieldValue;
                    } else {
                        throw new IllegalStateException("Nut supported time type: " + fieldValue);
                    }
                } else if (RowSchemaUtil.isLogicalTypeEnum(fieldType)) {
                    if(fieldValue instanceof EnumerationType.Value) {
                        yield ((EnumerationType.Value) fieldValue).getValue();
                    } else if(fieldValue instanceof Integer) {
                        yield fieldValue;
                    } else if(fieldValue instanceof String) {
                        yield fieldValue;
                    } else {
                        throw new IllegalArgumentException();
                    }
                } else {
                    throw new IllegalStateException();
                }
            }
            case MAP -> {
                final Map<String, Object> values = new HashMap<>();
                final Map<?,?> map = (Map<?,?>) fieldValue;
                for(final Map.Entry<?,?> entry : map.entrySet()) {
                    final Object value = getAsPrimitive(fieldType.getMapValueType(), entry.getValue());
                    values.put(entry.getKey().toString(), value);
                }
                yield values;
            }
            case ROW -> {
                final Map<String, Object> values = new HashMap<>();
                for (final Schema.Field field : fieldType.getRowSchema().getFields()) {
                    final Object value = getAsPrimitive(fieldValue, field.getType(), field.getName());
                    values.put(field.getName(), value);
                }
                yield values;
            }
            case ITERABLE, ARRAY -> switch (fieldType.getCollectionElementType().getTypeName()) {
                case INT16 -> ((List<Short>) fieldValue).stream()
                            .map(Short::intValue)
                            .collect(Collectors.toList());
                case INT32, INT64, FLOAT, DOUBLE, STRING, BOOLEAN, BYTES -> fieldValue;
                case DATETIME -> ((List<Instant>) fieldValue).stream()
                            .map(Instant::getMillis)
                            .map(l -> l * 1000L)
                            .collect(Collectors.toList());
                case LOGICAL_TYPE -> {
                    if (RowSchemaUtil.isLogicalTypeDate(fieldType.getCollectionElementType())) {
                        yield ((List<LocalDate>) fieldValue).stream()
                                .map(LocalDate::toEpochDay)
                                .collect(Collectors.toList());
                    } else if (RowSchemaUtil.isLogicalTypeTime(fieldType.getCollectionElementType())) {
                        yield ((List<LocalTime>) fieldValue).stream()
                                .map(LocalTime::toNanoOfDay)
                                .map(n -> n / 1000L)
                                .collect(Collectors.toList());
                    } else if (RowSchemaUtil.isLogicalTypeEnum(fieldType.getCollectionElementType())) {
                        yield ((List<EnumerationType.Value>) fieldValue).stream()
                                .map(EnumerationType.Value::getValue)
                                .collect(Collectors.toList());
                    } else {
                        throw new IllegalStateException();
                    }
                }
                case MAP -> ((List<Map<?,?>>) fieldValue).stream()
                        .map(map -> {
                            final Map<String, Object> values = new HashMap<>();
                            for(final Map.Entry<?,?> entry : map.entrySet()) {
                                final Object value = getAsPrimitive(fieldType.getCollectionElementType().getMapValueType(), entry.getValue());
                                values.put(entry.getKey().toString(), value);
                            }
                            return values;
                        })
                        .collect(Collectors.toList());
                case ROW -> ((List<Row>) fieldValue).stream()
                        .map(RowSchemaUtil::asPrimitiveMap)
                        .collect(Collectors.toList());
                default -> throw new IllegalStateException();
            };
            default -> throw new IllegalStateException("Not supported: " + fieldType + " for value: " + fieldValue);
        };

    }

    public static Object getAsStandard(final Object row, final String fieldName) {
        return getAsStandard((Row) row, fieldName);
    }

    public static Object getAsStandard(final Row row, final String fieldName) {
        if(row == null || fieldName == null) {
            return null;
        }
        if(!row.getSchema().hasField(fieldName)) {
            return null;
        }
        final Schema.Field field = row.getSchema().getField(fieldName);
        final Object value = row.getValue(fieldName);
        return getAsStandard(field.getType(), value);
    }

    public static Object getAsStandard(final Schema.FieldType fieldType, final Object value) {
        if(value == null) {
            return null;
        }
        return switch (fieldType.getTypeName()) {
            case BOOLEAN -> switch (value) {
                case Boolean b -> b;
                case String s -> Boolean.parseBoolean(s);
                case Number n -> n.doubleValue() > 0;
                default -> throw new IllegalArgumentException();
            };
            case STRING -> switch (value) {
                case String s -> s;
                case ByteBuffer bb -> new String(Base64.getDecoder().decode(bb.array()), StandardCharsets.UTF_8);
                case byte[] b -> new String(b, StandardCharsets.UTF_8);
                case Row row -> RowToJsonConverter.convert(row);
                case Object o -> o.toString();
            };
            case BYTES -> switch (value) {
                case ByteBuffer bb -> bb;
                case byte[] b -> ByteBuffer.wrap(b);
                case String s -> ByteBuffer.wrap(s.getBytes(StandardCharsets.UTF_8));
                default -> throw new IllegalArgumentException();
            };
            case INT32 -> switch (value) {
                case Number n -> n.intValue();
                case String s -> Integer.parseInt(s);
                case Boolean b -> b ? 1 : 0;
                default -> throw new IllegalArgumentException();
            };
            case INT64 -> switch (value) {
                case Number n -> n.longValue();
                case String s -> Long.parseLong(s);
                case Boolean b -> b ? 1L : 0L;
                default -> throw new IllegalArgumentException();
            };
            case FLOAT -> switch (value) {
                case Number n -> n.floatValue();
                case String s -> Float.parseFloat(s);
                case Boolean b -> b ? 1F : 0F;
                default -> throw new IllegalArgumentException();
            };
            case DOUBLE -> switch (value) {
                case Number n -> n.doubleValue();
                case String s -> Double.parseDouble(s);
                case Boolean b -> b ? 1D : 0D;
                default -> throw new IllegalArgumentException();
            };
            case LOGICAL_TYPE -> {
                if(isLogicalTypeDate(fieldType)) {
                    yield switch (value) {
                        case Number n -> LocalDate.ofEpochDay(n.longValue());
                        case String s -> DateTimeUtil.toLocalDate(s);
                        default -> throw new IllegalArgumentException();
                    };
                } else if(isLogicalTypeTime(fieldType)) {
                    yield switch (value) {
                        case Number n -> LocalTime.ofNanoOfDay(n.longValue() * 1000L);
                        case String s -> DateTimeUtil.toLocalTime(s);
                        default -> throw new IllegalArgumentException();
                    };
                } else if (isLogicalTypeEnum(fieldType)) {
                    final EnumerationType enumerationType = fieldType.getLogicalType(EnumerationType.class);
                    yield switch (value) {
                        case Number n -> enumerationType.valueOf(n.intValue()).toString();
                        case String s -> enumerationType.valueOf(s).toString();
                        default -> throw new IllegalArgumentException();
                    };
                } else {
                    throw new IllegalArgumentException();
                }
            }
            case DATETIME -> switch (value) {
                case Number n -> DateTimeUtil.toInstant(n.longValue());
                case String s -> DateTimeUtil.toInstant(s);
                default -> throw new IllegalArgumentException();
            };
            case ROW -> switch (value) {
                case Row row -> {
                    final Map<String, Object> standardValues = new HashMap<>();
                    for(final Schema.Field field : fieldType.getRowSchema().getFields()) {
                        standardValues.put(field.getName(), getAsStandard(field.getType(), (Object) row.getValue(field.getName())));
                    }
                    yield standardValues;
                }
                default -> throw new IllegalArgumentException();
            };
            case ARRAY, ITERABLE -> switch (value) {
                case List list -> {
                    final List<Object> standardValues = new ArrayList<>();
                    for(final Object v : list) {
                        final Object standardValue = getAsStandard(fieldType.getCollectionElementType(), v);
                        standardValues.add(standardValue);
                    }
                    yield  standardValues;
                }
                default -> throw new IllegalArgumentException();
            };
            default -> throw new IllegalArgumentException();
        };
    }

    public static List<Float> getAsFloatList(final Row row, final String fieldName) {
        final Schema.Field field = row.getSchema().getField(fieldName);
        if(field == null) {
            return new ArrayList<>();
        }
        if(!row.getSchema().hasField(fieldName)) {
            return new ArrayList<>();
        }
        if(!field.getType().getTypeName().equals(Schema.TypeName.ARRAY)) {
            return new ArrayList<>();
        }
        final List<?> list = row.getValue(fieldName);
        if(list == null) {
            return new ArrayList<>();
        }

        return switch (field.getType().getCollectionElementType().getTypeName()) {
            case STRING -> list.stream()
                    .map(o -> (String) o)
                    .map(Float::valueOf)
                    .collect(Collectors.toList());
            case INT16 -> list.stream()
                    .map(o -> (Short) o)
                    .map(Float::valueOf)
                    .collect(Collectors.toList());
            case INT32 -> list.stream()
                    .map(o -> (Integer) o)
                    .map(Float::valueOf)
                    .collect(Collectors.toList());
            case INT64 -> list.stream()
                    .map(o -> (Long) o)
                    .map(Float::valueOf)
                    .collect(Collectors.toList());
            case FLOAT -> list.stream()
                    .map(o -> (Float) o)
                    .collect(Collectors.toList());
            case DOUBLE -> list.stream()
                    .map(o -> (Double) o)
                    .map(Double::floatValue)
                    .collect(Collectors.toList());
            default -> throw new IllegalStateException();
        };

    }

    public static Object convertPrimitive(Schema.FieldType fieldType, Object primitiveValue) {
        if(primitiveValue == null) {
            return null;
        }
        return switch (fieldType.getTypeName()) {
            case INT16 -> switch (primitiveValue) {
                case Short s -> s;
                case String string -> Short.valueOf(string);
                case Number n -> n.shortValue();
                default -> throw new IllegalArgumentException();
            };
            case INT32 -> switch (primitiveValue) {
                case Integer i -> i;
                case String string -> Integer.valueOf(string);
                case Number n -> n.intValue();
                default -> throw new IllegalArgumentException();
            };
            case INT64 -> switch (primitiveValue) {
                case Long l -> l;
                case String string -> Long.valueOf(string);
                case Number n -> n.longValue();
                default -> throw new IllegalArgumentException();
            };
            case FLOAT -> switch (primitiveValue) {
                case Float f -> f;
                case String string -> Float.valueOf(string);
                case Number n -> n.floatValue();
                default -> throw new IllegalArgumentException();
            };
            case DOUBLE -> switch (primitiveValue) {
                case Double d -> d;
                case String string -> Double.valueOf(string);
                case Number n -> n.doubleValue();
                default -> throw new IllegalArgumentException();
            };
            case STRING -> switch (primitiveValue) {
                case String s -> s;
                default -> primitiveValue.toString();
            };
            case BYTES -> switch (primitiveValue) {
                case byte[] bytes -> bytes;
                case String s -> s.getBytes(StandardCharsets.UTF_8);
                default -> primitiveValue.toString().getBytes(StandardCharsets.UTF_8);
            };
            case BOOLEAN -> switch (primitiveValue) {
                case Boolean b -> b;
                case String s -> Boolean.parseBoolean(s);
                case Number n -> n.doubleValue() > 0;
                default -> throw new IllegalArgumentException();
            };
            case DATETIME -> switch (primitiveValue) {
                case Instant i -> i;
                case Long l -> Instant.ofEpochMilli(l / 1000L);
                case Number n -> Instant.ofEpochMilli(n.longValue() / 1000L);
                default -> throw new IllegalArgumentException();
            };
            case LOGICAL_TYPE -> {
                if (RowSchemaUtil.isLogicalTypeDate(fieldType)) {
                    yield LocalDate.ofEpochDay((Integer) primitiveValue);
                } else if (RowSchemaUtil.isLogicalTypeTime(fieldType)) {
                    yield LocalTime.ofNanoOfDay(1000L * (Long) primitiveValue);
                } else if (RowSchemaUtil.isLogicalTypeEnum(fieldType)) {
                    if(primitiveValue instanceof Integer) {
                        final int index = (Integer) primitiveValue;
                        yield fieldType.getLogicalType(EnumerationType.class).valueOf(index);
                    } else if(primitiveValue instanceof EnumerationType.Value) {
                        yield primitiveValue;
                    } else {
                        throw new IllegalStateException();
                    }
                } else {
                    throw new IllegalStateException();
                }
            }
            case MAP -> {
                final Map<String, Object> output = new HashMap<>();
                if(primitiveValue instanceof Map) {
                    final Map<String, Object> map = (Map<String, Object>) primitiveValue;
                    for(final Map.Entry<String, Object> entry : map.entrySet()) {
                        output.put(entry.getKey(), convertPrimitive(fieldType.getMapValueType(), entry.getValue()));
                    }
                } else if(primitiveValue instanceof Row || primitiveValue instanceof String) {
                    final Row row;
                    if(primitiveValue instanceof Row) {
                        row = (Row) primitiveValue;
                    } else {
                        row = JsonToRowConverter.convert(fieldType.getRowSchema(), (String) primitiveValue);
                    }
                    for(final Schema.Field field : row.getSchema().getFields()) {
                        output.put(field.getName(), row.getValue(field.getName()));
                    }
                } else {
                    throw new IllegalStateException();
                }
                yield output;
            }
            case ROW -> {
                if(primitiveValue instanceof Row) {
                    yield primitiveValue;
                } else if(primitiveValue instanceof Map) {
                    final Map<String, Object> map = (Map<String, Object>) primitiveValue;
                    yield convertPrimitives(fieldType.getRowSchema(), map);
                } else if(primitiveValue instanceof String) {
                    yield JsonToRowConverter.convert(fieldType.getRowSchema(), (String) primitiveValue);
                } else {
                    throw new IllegalStateException();
                }
            }
            case ITERABLE, ARRAY ->
                switch (fieldType.getCollectionElementType().getTypeName()) {
                    case INT16 -> ((List<Integer>) primitiveValue).stream()
                                .map(Integer::shortValue)
                                .collect(Collectors.toList());
                    case INT32, INT64, FLOAT, DOUBLE, STRING, BOOLEAN -> primitiveValue;
                    case DATETIME -> ((List<Long>) primitiveValue).stream()
                                .map(l -> l / 1000L)
                                .map(Instant::ofEpochMilli)
                                .collect(Collectors.toList());
                    case LOGICAL_TYPE -> {
                        if (RowSchemaUtil.isLogicalTypeDate(fieldType.getCollectionElementType())) {
                            yield ((List<Integer>) primitiveValue).stream()
                                    .map(LocalDate::ofEpochDay)
                                    .collect(Collectors.toList());
                        } else if (RowSchemaUtil.isLogicalTypeTime(fieldType.getCollectionElementType())) {
                            yield ((List<Long>) primitiveValue).stream()
                                    .map(l -> l * 1000)
                                    .map(LocalTime::ofNanoOfDay)
                                    .collect(Collectors.toList());
                        } else if (RowSchemaUtil.isLogicalTypeEnum(fieldType.getCollectionElementType())) {
                            yield ((List<Integer>) primitiveValue).stream()
                                    .map(index -> fieldType.getLogicalType(EnumerationType.class).valueOf(index))
                                    .collect(Collectors.toList());
                        } else {
                            throw new IllegalStateException();
                        }
                    }
                    case ROW -> ((List<?>) primitiveValue).stream()
                                .map(o -> {
                                    if(o instanceof Row) {
                                        return o;
                                    } else if(o instanceof Map) {
                                        final Map<String, Object> map = (Map<String, Object>) o;
                                        return convertPrimitives(fieldType.getCollectionElementType().getRowSchema(), map);
                                    } else if(o instanceof String) {
                                        return JsonToRowConverter.convert(fieldType.getCollectionElementType().getRowSchema(), ((String) o));
                                    } else {
                                        throw new IllegalStateException();
                                    }
                                })
                                .collect(Collectors.toList());
                    default -> throw new IllegalStateException();
                };
            default -> throw new IllegalStateException("Not supported fieldType: " + fieldType);
        };
    }

    public static Row convertPrimitives(final Schema rowSchema, final Map<String, Object> map) {
        final Row.FieldValueBuilder builder = Row.withSchema(rowSchema).withFieldValues(new HashMap<>());
        for(final Schema.Field field : rowSchema.getFields()) {
            final Object value = convertPrimitive(field.getType(), map.get(field.getName()));
            builder.withFieldValue(field.getName(), value);
        }
        return builder.build();
    }

    public static Map<String, Object> asPrimitiveMap(final Row row) {
        final Map<String, Object> primitiveMap = new HashMap<>();
        if(row == null) {
            return primitiveMap;
        }
        for(final Schema.Field field : row.getSchema().getFields()) {
            final Object value = getAsPrimitive(row, field.getType(), field.getName());
            primitiveMap.put(field.getName(), value);
        }
        return primitiveMap;
    }

    public static Map<String, Object> asStandardMap(final Row row, final Collection<String> fieldNames) {
        final Map<String, Object> standardMap = new HashMap<>();
        if(row == null) {
            return standardMap;
        }
        for(final Schema.Field field : row.getSchema().getFields()) {
            if(fieldNames != null && !fieldNames.isEmpty() && !fieldNames.contains(field.getName())) {
                continue;
            }
            final Object value = getAsStandard(row, field.getName());
            standardMap.put(field.getName(), value);
        }
        return standardMap;
    }

    public static Instant toInstant(final Object value) {
        return (Instant) value;
    }

    public static Instant toTimestampValue(final Instant timestamp) {
        return timestamp;
    }

    public static EnumerationType.Value toEnumerationTypeValue(final Schema.FieldType fieldType, final String value) {
        final EnumerationType enumerationType = fieldType.getLogicalType(EnumerationType.class);
        final int typeCode = enumerationType
                .getArgument()
                .getOrDefault(value, enumerationType.getValues().size() - 1);
        return new EnumerationType.Value(typeCode);
    }

    public static String toString(final Schema.FieldType fieldType, final EnumerationType.Value value) {
        return fieldType.getLogicalType(EnumerationType.class).toString(value);
    }

    public static Schema.Options createDefaultValueOptions(final String defaultValue) {
        return Schema.Options.builder()
                .setOption(RowSchemaUtil.OPTION_NAME_DEFAULT_VALUE, Schema.FieldType.STRING, defaultValue)
                .build();
    }

    public static Object getDefaultValue(final Schema.FieldType fieldType, final Schema.Options fieldOptions) {
        if(fieldOptions == null || !fieldOptions.hasOption(OPTION_NAME_DEFAULT_VALUE)) {
            return null;
        }
        final String defaultValue = fieldOptions.getValue(OPTION_NAME_DEFAULT_VALUE, String.class);
        return convertDefaultValue(fieldType, defaultValue);
    }

    private static Object convertDefaultValue(final Schema.FieldType fieldType, final String defaultValue) {
        return switch (fieldType.getTypeName()) {
            case STRING -> defaultValue;
            case BOOLEAN -> Boolean.valueOf(defaultValue);
            case BYTE -> Byte.valueOf(defaultValue);
            case INT16 -> Short.valueOf(defaultValue);
            case INT32 -> Integer.valueOf(defaultValue);
            case INT64 -> Long.valueOf(defaultValue);
            case FLOAT -> Float.valueOf(defaultValue);
            case DOUBLE -> Double.valueOf(defaultValue);
            case DECIMAL -> new BigDecimal(defaultValue);
            case BYTES -> Base64.getDecoder().decode(defaultValue);
            case DATETIME -> DateTimeUtil.toJodaInstant(defaultValue);
            case LOGICAL_TYPE -> {
                final JsonElement element = new Gson().fromJson(defaultValue, JsonElement.class);
                if(!element.isJsonPrimitive()) {
                    yield null;
                }
                final JsonPrimitive primitive = element.getAsJsonPrimitive();
                if(RowSchemaUtil.isLogicalTypeDate(fieldType)) {
                    if(primitive.isString()) {
                        yield DateTimeUtil.toLocalDate(primitive.getAsString());
                    } else if(primitive.isNumber()) {
                        yield LocalDate.ofEpochDay(primitive.getAsLong());
                    } else {
                        throw new IllegalStateException("json fieldType: " + fieldType.getTypeName() + ", value: " + primitive + " could not be convert to date");
                    }
                } else if(RowSchemaUtil.isLogicalTypeTime(fieldType)) {
                    if(primitive.isString()) {
                        yield DateTimeUtil.toLocalTime(primitive.getAsString());
                    } else if(primitive.isNumber()) {
                        yield LocalTime.ofSecondOfDay(primitive.getAsLong());
                    } else {
                        throw new IllegalStateException("json fieldType: " + fieldType.getTypeName() + ", value: " + primitive + " could not be convert to time");
                    }
                } else if(RowSchemaUtil.isLogicalTypeTimestamp(fieldType)) {
                    if(primitive.isString()) {
                        yield DateTimeUtil.toJodaInstant(primitive.getAsString());
                    } else if(primitive.isNumber()) {
                        yield DateTimeUtil.toJodaInstant(primitive.getAsLong());
                    } else {
                        final String message = "json fieldType: " + fieldType.getTypeName() + ", value: " + primitive + " could not be convert to timestamp";
                        throw new IllegalStateException(message);
                    }
                } else if(RowSchemaUtil.isLogicalTypeEnum(fieldType)) {
                    final String enumString = primitive.getAsString();
                    yield RowSchemaUtil.toEnumerationTypeValue(fieldType, enumString);
                } else {
                    throw new IllegalArgumentException(
                            "Unsupported Beam logical type: " + fieldType.getLogicalType().getIdentifier());
                }
            }
            case ROW -> {
                final JsonElement element = new Gson().fromJson(defaultValue, JsonElement.class);
                yield JsonToRowConverter.convert(fieldType.getRowSchema(), element);
            }
            default -> throw new IllegalStateException("Not supported default value type: " + fieldType.getTypeName());
        };
    }

}
