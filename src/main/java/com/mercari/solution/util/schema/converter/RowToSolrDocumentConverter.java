package com.mercari.solution.util.schema.converter;

import com.mercari.solution.util.schema.RowSchemaUtil;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.logicaltypes.EnumerationType;
import org.apache.beam.sdk.values.Row;
import org.apache.solr.common.SolrInputDocument;

import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.List;
import java.util.Objects;

public class RowToSolrDocumentConverter {

    public static SolrInputDocument convert(final Row row, final List<String> fieldNames) {
        final SolrInputDocument doc = new SolrInputDocument();
        for(final org.apache.beam.sdk.schemas.Schema.Field field : row.getSchema().getFields()) {
            if(fieldNames != null && !fieldNames.contains(field.getName())) {
                continue;
            }
            setFieldValue(doc, field.getName(), field.getType(), row, fieldNames);
        }
        return doc;
    }

    private static void setFieldValue(final SolrInputDocument doc,
                                      final String fieldName, final Schema.FieldType fieldType,
                                      final Row row,
                                      final List<String> fieldNames) {

        final boolean isNullField = row.getValue(fieldName) == null;
        switch (fieldType.getTypeName()) {
            case BOOLEAN: {
                doc.addField(fieldName, isNullField ? (Boolean)false : row.getBoolean(fieldName));
                return;
            }
            case STRING: {
                doc.addField(fieldName, isNullField ? "" : row.getString(fieldName));
                return;
            }
            case BYTES: {
                doc.addField(fieldName, isNullField ? new byte[0] : row.getBytes(fieldName));
                return;
            }
            case INT16: {
                doc.addField(fieldName, isNullField ? 0 : row.getInt16(fieldName));
                return;
            }
            case INT32: {
                doc.addField(fieldName, isNullField ? 0 : row.getInt32(fieldName));
                return;
            }
            case INT64: {
                doc.addField(fieldName, isNullField ? 0L : row.getInt64(fieldName));
                return;
            }
            case FLOAT: {
                doc.addField(fieldName, isNullField ? 0F : row.getFloat(fieldName));
                return;
            }
            case DOUBLE: {
                doc.addField(fieldName, isNullField ? 0D : row.getDouble(fieldName));
                return;
            }
            case DATETIME: {
                doc.addField(fieldName, isNullField ? new Date(0) : row.getDateTime(fieldName).toInstant().toDate());
                return;
            }
            case ITERABLE:
            case ARRAY: {
                setArrayFieldValue(doc, fieldName, fieldType.getCollectionElementType(), row);
                return;
            }
            case ROW: {
                return;
            }
            case LOGICAL_TYPE: {
                if (RowSchemaUtil.isLogicalTypeDate(fieldType)) {
                    final LocalDate localDate = row.getLogicalTypeValue(fieldName, LocalDate.class);
                    final Date date = Date.from(localDate.atStartOfDay(ZoneOffset.UTC).toInstant());
                    doc.addField(fieldName, isNullField ? new Date(0) : date);
                    return;
                } else if (RowSchemaUtil.isLogicalTypeTime(fieldType)) {
                    // TODO
                    doc.addField(fieldName, isNullField ? "" : "");
                    return;
                } else if (RowSchemaUtil.isLogicalTypeEnum(fieldType)) {
                    final EnumerationType.Value enumValue = row.getLogicalTypeValue(fieldName, EnumerationType.Value.class);
                    doc.addField(fieldName, isNullField ? "" : RowSchemaUtil.toString(fieldType, enumValue));
                    return;
                } else {
                    throw new IllegalArgumentException(
                            "Unsupported Beam logical type: " + fieldType.getLogicalType().getIdentifier());
                }
            }
            case MAP:
            case BYTE:
            case DECIMAL:
            default:
                break;

        }
    }

    private static void setArrayFieldValue(final SolrInputDocument doc, final String fieldName, final Schema.FieldType fieldType, final Row row) {
        if(row.getValue(fieldName) == null) {
            return;
        }
        final Object value = row.getValue(fieldName);
        switch (fieldType.getTypeName()) {
            case BOOLEAN:
                ((List<Boolean>)value).stream()
                        .filter(b -> b != null)
                        .forEach(b -> doc.addField(fieldName, b));
                return;
            case STRING:
                ((List<Object>) value).stream()
                        .filter(Objects::nonNull)
                        .map(utf8 -> utf8.toString())
                        .forEach(s -> doc.addField(fieldName, s));
                return;
            case BYTES:
                ((List<ByteBuffer>) value).stream()
                        .filter(Objects::nonNull)
                        .map(bytes -> bytes.array())
                        .forEach(b -> doc.addField(fieldName, b));
                return;
            case INT32:
                ((List<Integer>) value).stream()
                        .filter(Objects::nonNull)
                        .forEach(i -> doc.addField(fieldName, i));
                return;
            case INT64:
                ((List<Long>) value).stream()
                        .filter(Objects::nonNull)
                        .forEach(i -> doc.addField(fieldName, i));
                return;
            case FLOAT:
                ((List<Float>) value).stream()
                        .filter(Objects::nonNull)
                        .forEach(i -> doc.addField(fieldName, i));
                return;
            case DOUBLE:
                ((List<Double>) value).stream()
                        .filter(Objects::nonNull)
                        .forEach(i -> doc.addField(fieldName, i));
                return;
            case LOGICAL_TYPE:
                if(RowSchemaUtil.isLogicalTypeDate(fieldType)) {
                    ((List<Integer>) value).stream()
                            .filter(Objects::nonNull)
                            .map(epochDays -> new Date(epochDays * 86400000L))
                            .forEach(s -> doc.addField(fieldName, s));
                    return;
                } else if(RowSchemaUtil.isLogicalTypeTime(fieldType)) {
                    ((List<Integer>) value).stream()
                            .filter(Objects::nonNull)
                            .map(second -> new Long(second) * 1000 * 1000)
                            .map(ms -> LocalTime.ofNanoOfDay(ms).format(DateTimeFormatter.ISO_LOCAL_TIME))
                            .forEach(s -> doc.addField(fieldName, s));
                    return;
                } else if(RowSchemaUtil.isLogicalTypeTimestamp(fieldType)) {
                    ((List<Long>) value).stream()
                            .filter(Objects::nonNull)
                            .map(l -> new Date(l/1000L))
                            .forEach(s -> doc.addField(fieldName, s));
                    return;
                } else if(RowSchemaUtil.isLogicalTypeEnum(fieldType)) {
                    ((List<EnumerationType.Value>) value).stream()
                            .filter(Objects::nonNull)
                            .map(v -> RowSchemaUtil.toString(fieldType, v))
                            .forEach(s -> doc.addField(fieldName, s));
                    return;
                } else {
                    throw new IllegalArgumentException(
                            "Unsupported Beam logical type: " + fieldType.getLogicalType().getIdentifier());
                }
            default:
                return;
        }
    }

}
