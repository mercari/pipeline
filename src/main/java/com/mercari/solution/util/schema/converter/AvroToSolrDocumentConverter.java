package com.mercari.solution.util.schema.converter;

/*
import com.mercari.solution.util.schema.AvroSchemaUtil;
import com.mercari.solution.util.schema.SolrSchemaUtil;
import com.mercari.solution.util.domain.text.XmlUtil;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.io.gcp.bigquery.SchemaAndRecord;
import org.apache.solr.common.SolrInputDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.List;
import java.util.Objects;
 */

public class AvroToSolrDocumentConverter {

    /*
    private static final Logger LOG = LoggerFactory.getLogger(AvroToSolrDocumentConverter.class);

    public static SolrInputDocument convert(final GenericRecord record, final List<String> fieldNames) {
        return convert(record.getSchema(), record, fieldNames);
    }

    public static SolrInputDocument convert(final SchemaAndRecord schemaAndRecord) {
        if(schemaAndRecord.getTableSchema() == null) {
            return convert(schemaAndRecord.getRecord().getSchema(), schemaAndRecord.getRecord(), null);
        }
        final Schema schema = AvroSchemaUtil.convertSchema(schemaAndRecord.getTableSchema());
        return convert(schema, schemaAndRecord.getRecord(), null);
    }

    public static SolrInputDocument convert(final Schema schema, final GenericRecord record, final List<String> fieldNames) {
        return convert(schema, record, null, fieldNames);
    }

    public static SolrInputDocument convert(final Schema schema, final GenericRecord record, final String parentName, final List<String> fieldNames) {
        final SolrInputDocument doc = new SolrInputDocument();
        for(final Schema.Field field : schema.getFields()) {
            setFieldValue(doc, parentName, field.name(), field.schema(), record, fieldNames);
        }
        return doc;
    }

    public static String convertSchema(final Schema schema) {
        final Document document = SolrSchemaUtil.createSchemaXML("content");

        // fields
        final Element fields = document.createElement("fields");
        for(final Schema.Field field : schema.getFields()) {
            setSchemaField(document, fields, field.name(), field.schema(), true, false);
        }

        // types
        final Element types = document.createElement("types");
        SolrSchemaUtil.setDefaultSchemaTypes(document, types);

        final Element root = document.getDocumentElement();
        root.appendChild(fields);
        root.appendChild(types);
        return XmlUtil.toString(document);
    }

    private static void setFieldValue(final SolrInputDocument doc,
                                      final String parentName,
                                      final String fieldName,
                                      final Schema schema,
                                      final GenericRecord record,
                                      final List<String> fieldNames) {

        final Object value = record.hasField(fieldName) ? record.get(fieldName) : null;
        final boolean isNullField = value == null;
        final String name = parentName == null ? fieldName : parentName + "." + fieldName;

        if(fieldNames != null && !fieldNames.contains(name)) {
            return;
        }

        switch (schema.getType()) {
            case BOOLEAN -> doc.addField(name, isNullField ? false : (Boolean)value);
            case ENUM, STRING -> doc.addField(name, isNullField ? "" : value.toString());
            case FIXED -> doc.addField(name, isNullField ? new byte[0] : ((GenericData.Fixed)value).bytes());
            case BYTES -> doc.addField(name, isNullField ? new byte[0] : ((ByteBuffer)value).array());
            case INT -> {
                final Integer intValue = (Integer) value;
                if (LogicalTypes.date().equals(schema.getLogicalType())) {
                    if(intValue == null) {
                        doc.addField(name, "");
                    } else {
                        final LocalDate ld = LocalDate.ofEpochDay(intValue);
                        final Date date = Date.from(ld.atStartOfDay().toInstant(ZoneOffset.UTC));
                        doc.addField(name, date);
                    }
                } else if (LogicalTypes.timeMillis().equals(schema.getLogicalType())) {
                    doc.addField(name, isNullField ? "" : new Date(intValue).toString());
                } else {
                    doc.addField(name, isNullField ? 0 : intValue);
                }
            }
            case LONG -> {
                final Long longValue = (Long) value;
                if (LogicalTypes.timestampMillis().equals(schema.getLogicalType())) {
                    final Date date = isNullField ? new Date(0) : new Date(longValue);
                    doc.addField(name, date);
                } else if (LogicalTypes.timestampMicros().equals(schema.getLogicalType())) {
                    final Date date = isNullField ? new Date(0) : new Date(longValue / 1000L);
                    doc.addField(name, date);
                } else if (LogicalTypes.timeMicros().equals(schema.getLogicalType())) {
                    //TODO
                    doc.addField(name, isNullField ? 0L : longValue);
                } else {
                    doc.addField(name, isNullField ? 0L : longValue);
                }
            }
            case FLOAT -> doc.addField(name, isNullField ? 0F : (Float)value);
            case DOUBLE -> doc.addField(name, isNullField ? 0D : (Double)value);
            case RECORD -> {
                if (!isNullField) {
                    doc.addChildDocument(convert(schema, (GenericRecord) value, name, fieldNames));
                }
            }
            case ARRAY -> setArrayFieldValue(doc, parentName, fieldName, AvroSchemaUtil.unnestUnion(schema.getElementType()), record, fieldNames);
            case UNION -> setFieldValue(doc, parentName, fieldName, AvroSchemaUtil.unnestUnion(schema), record, fieldNames);
        }
    }

    private static void setArrayFieldValue(final SolrInputDocument doc,
                                           final String parentName,
                                           final String fieldName,
                                           final Schema schema,
                                           final GenericRecord record,
                                           final List<String> fieldNames) {

        final Object value = record.hasField(fieldName) ? record.get(fieldName) : null;
        if(value == null) {
            return;
        }
        final String name = parentName == null ? fieldName : parentName + "." + fieldName;
        if(fieldNames != null && !fieldNames.contains(name)) {
            return;
        }

        switch (schema.getType()) {
            case BOOLEAN -> ((List<Boolean>)value).stream()
                        .filter(Objects::nonNull)
                        .forEach(b -> doc.addField(name, b));
            case ENUM, STRING ->
                ((List<Object>)value).stream()
                        .filter(Objects::nonNull)
                        .map(Object::toString)
                        .forEach(s -> doc.addField(name, s));
            case FIXED -> ((List<GenericData.Fixed>)value).stream()
                        .filter(Objects::nonNull)
                        .map(GenericData.Fixed::bytes)
                        .forEach(s -> doc.addField(name, s));
            case BYTES -> ((List<ByteBuffer>) value).stream()
                        .filter(Objects::nonNull)
                        .map(ByteBuffer::array)
                        .forEach(b -> doc.addField(name, b));
            case INT -> {
                if (LogicalTypes.date().equals(schema.getLogicalType())) {
                    ((List<Integer>) value).stream()
                            .filter(Objects::nonNull)
                            .map(LocalDate::ofEpochDay)
                            .map(LocalDate::atStartOfDay)
                            .map(ldt -> ldt.toInstant(ZoneOffset.UTC))
                            .map(Date::from)
                            .forEach(d -> doc.addField(name, d));
                    return;
                } else if (LogicalTypes.timeMillis().equals(schema.getLogicalType())) {
                    ((List<Integer>) value).stream()
                            .filter(Objects::nonNull)
                            .map(second -> Long.valueOf(second) * 1000 * 1000)
                            .map(ms -> LocalTime.ofNanoOfDay(ms).format(DateTimeFormatter.ISO_LOCAL_TIME))
                            .forEach(s -> doc.addField(name, s));
                    return;
                }
                ((List<Integer>) value).stream()
                        .filter(Objects::nonNull)
                        .forEach(i -> doc.addField(name, i));
            }
            case LONG -> {
                if (LogicalTypes.timestampMillis().equals(schema.getLogicalType())) {
                    ((List<Long>) value).stream()
                            .filter(Objects::nonNull)
                            .map(Date::new)
                            .forEach(s -> doc.addField(name, s));
                    return;
                } else if (LogicalTypes.timestampMicros().equals(schema.getLogicalType())) {
                    ((List<Long>) value).stream()
                            .filter(Objects::nonNull)
                            .map(l -> l / 1000)
                            .map(Date::new)
                            .forEach(s -> doc.addField(name, s));
                    return;
                }
                ((List<Long>) value).stream()
                        .filter(Objects::nonNull)
                        .forEach(i -> doc.addField(name, i));
            }
            case FLOAT ->
                ((List<Float>) value).stream()
                        .filter(Objects::nonNull)
                        .forEach(i -> doc.addField(name, i));
            case DOUBLE ->
                ((List<Double>) value).stream()
                        .filter(Objects::nonNull)
                        .forEach(i -> doc.addField(name, i));
            case RECORD ->
                ((List<GenericRecord>) value).stream()
                        .filter(Objects::nonNull)
                        .forEach(r -> doc.addChildDocument(convert(schema, r, fieldName, fieldNames)));
            case UNION -> setArrayFieldValue(doc, parentName, fieldName, AvroSchemaUtil.unnestUnion(schema), record, fieldNames);
        }
    }

    private static void setSchemaField(final Document document,
                                       final Element fields,
                                       final String name,
                                       final Schema schema,
                                       final boolean nullable,
                                       final boolean repeated) {

        final Element fieldElement = document.createElement("field");
        fieldElement.setAttribute("name", name);
        fieldElement.setAttribute("indexed", "true");
        fieldElement.setAttribute("stored", "true");
        fieldElement.setAttribute("required", nullable ? "false" : "true");
        if(repeated) {
            fieldElement.setAttribute("multiValued", "true");
        }
        switch (schema.getType()) {
            case BOOLEAN -> fieldElement.setAttribute("type", "boolean");
            case STRING -> fieldElement.setAttribute("type", "textja");
            case ENUM -> fieldElement.setAttribute("type", "string");
            case FLOAT -> fieldElement.setAttribute("type", "float");
            case DOUBLE -> fieldElement.setAttribute("type", "double");
            case INT -> {
                if(LogicalTypes.date().equals(schema.getLogicalType())) {
                    fieldElement.setAttribute("type", "date");
                } else if(LogicalTypes.timeMillis().equals(schema.getLogicalType())) {
                    fieldElement.setAttribute("type", "string");
                } else {
                    fieldElement.setAttribute("type", "int");
                }
            }
            case LONG -> {
                if(LogicalTypes.timestampMillis().equals(schema.getLogicalType())) {
                    fieldElement.setAttribute("type", "date");
                } else if(LogicalTypes.timestampMicros().equals(schema.getLogicalType())) {
                    fieldElement.setAttribute("type", "date");
                } else if(LogicalTypes.timeMicros().equals(schema.getLogicalType())) {
                    fieldElement.setAttribute("type", "string");
                } else {
                    fieldElement.setAttribute("type", "long");
                }
            }
            case ARRAY -> setSchemaField(document, fields, name, schema.getElementType(), nullable, true);
            case UNION -> setSchemaField(document, fields, name, AvroSchemaUtil.unnestUnion(schema), true, repeated);
        }
        fields.appendChild(fieldElement);
    }

     */

}
