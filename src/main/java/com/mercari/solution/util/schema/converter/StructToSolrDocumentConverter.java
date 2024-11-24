package com.mercari.solution.util.schema.converter;

import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Type;
import com.mercari.solution.util.schema.SolrSchemaUtil;
import com.mercari.solution.util.domain.text.XmlUtil;
import org.apache.solr.common.SolrInputDocument;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Objects;

public class StructToSolrDocumentConverter {

    public static SolrInputDocument convert(final Struct struct, final List<String> fieldNames) {
        return convert(struct, null, fieldNames);
    }

    public static SolrInputDocument convert(final Struct struct, final String parentName, final List<String> fieldNames) {
        final SolrInputDocument doc = new SolrInputDocument();
        for(final Type.StructField field : struct.getType().getStructFields()) {
            if(fieldNames != null && !fieldNames.contains(field.getName())) {
                continue;
            }
            setFieldValue(doc, parentName, field.getName(), field.getType(), struct, fieldNames);
        }
        return doc;
    }

    public static String convertSchema(final Type type) {
        final Document document = SolrSchemaUtil.createSchemaXML("content");

        // fields
        final Element fields = document.createElement("fields");
        for(final Type.StructField field : type.getStructFields()) {
            setSchemaField(document, fields, field.getName(), field.getType(), true, false);
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
                                      final String parentName, final String fieldName,
                                      final Type type, final Struct struct,
                                      final List<String> fieldNames) {

        final boolean isNullField = struct.isNull(fieldName);
        final String name = parentName == null ? fieldName : parentName + "." + fieldName;
        switch (type.getCode()) {
            case BOOL -> doc.addField(name, !isNullField && struct.getBoolean(fieldName));
            case STRING -> doc.addField(name, isNullField ? "" : struct.getString(fieldName));
            case BYTES -> doc.addField(name, isNullField ? new byte[0] : struct.getBytes(fieldName).toByteArray());
            case INT64 -> doc.addField(name, isNullField ? 0L : struct.getLong(fieldName));
            case FLOAT32 -> doc.addField(name, isNullField ? 0D : struct.getFloat(fieldName));
            case FLOAT64 -> doc.addField(name, isNullField ? 0D : struct.getDouble(fieldName));
            case DATE -> {
                final com.google.cloud.Date date = struct.getDate(fieldName);
                final LocalDate ld = LocalDate.of(date.getYear(), date.getMonth(), date.getDayOfMonth());
                doc.addField(name, isNullField ? "" : java.util.Date.from(ld.atStartOfDay().toInstant(ZoneOffset.UTC)));
            }
            case TIMESTAMP -> doc.addField(name, isNullField ? "" : struct.getTimestamp(fieldName).toDate());
            case STRUCT -> {
                if (!isNullField) {
                    doc.addChildDocument(convert(struct.getStruct(fieldName), name, fieldNames));
                }
            }
            case ARRAY -> setArrayFieldValue(doc, parentName, name, struct.getType().getArrayElementType(), struct, fieldNames);
            case NUMERIC -> doc.addField(name, isNullField ? 0D : struct.getBigDecimal(fieldName).doubleValue());
        }
    }

    private static void setArrayFieldValue(final SolrInputDocument doc,
                                           final String parentName, final String fieldName,
                                           final Type type, final Struct struct,
                                           final List<String> fieldNames) {

        if(struct.isNull(fieldName)) {
            return;
        }
        final String name = parentName == null ? fieldName : parentName + "." + fieldName;
        switch (type.getCode()) {
            case BOOL -> struct.getBooleanList(fieldName).stream()
                        .filter(Objects::nonNull)
                        .forEach(b -> doc.addField(name, b));
            case STRING -> struct.getStringList(fieldName).stream()
                        .filter(Objects::nonNull)
                        .forEach(s -> doc.addField(name, s));
            case BYTES -> struct.getBytesList(fieldName).stream()
                        .filter(Objects::nonNull)
                        .map(bytes -> bytes.toByteArray())
                        .forEach(b -> doc.addField(name, b));
            case INT64 -> struct.getLongList(fieldName).stream()
                        .filter(Objects::nonNull)
                        .forEach(i -> doc.addField(name, i));
            case FLOAT32 -> struct.getFloatList(fieldName).stream()
                        .filter(Objects::nonNull)
                        .forEach(i -> doc.addField(name, i));
            case FLOAT64 -> struct.getDoubleList(fieldName).stream()
                    .filter(Objects::nonNull)
                    .forEach(i -> doc.addField(name, i));
            case DATE -> struct.getDateList(fieldName).stream()
                        .filter(Objects::nonNull)
                        .map(d -> {
                            final com.google.cloud.Date date = struct.getDate(fieldName);
                            final LocalDate ld = LocalDate.of(date.getYear(), date.getMonth(), date.getDayOfMonth());
                            return java.util.Date.from(ld.atStartOfDay().toInstant(ZoneOffset.UTC));
                        })
                        .forEach(s -> doc.addField(name, s));
            case TIMESTAMP -> struct.getTimestampList(fieldName).stream()
                        .filter(Objects::nonNull)
                        .map(Timestamp::toDate)
                        .forEach(i -> doc.addField(name, i));
            case STRUCT -> struct.getStructList(fieldName).stream()
                        .filter(Objects::nonNull)
                        .map(s -> convert(s, name, fieldNames))
                        .forEach(doc::addChildDocument);
            case NUMERIC -> struct.getBigDecimalList(fieldName).stream()
                    .filter(Objects::nonNull)
                    .forEach(i -> doc.addField(name, i.doubleValue()));
        }
    }

    private static void setSchemaField(final Document document,
                                       final Element fields,
                                       final String name,
                                       final Type type,
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
        switch (type.getCode()) {
            case STRING -> fieldElement.setAttribute("type", "textja");
            case BOOL -> fieldElement.setAttribute("type", "boolean");
            case INT64 -> fieldElement.setAttribute("type", "long");
            case FLOAT32 -> fieldElement.setAttribute("type", "float");
            case FLOAT64 -> fieldElement.setAttribute("type", "double");
            case DATE -> fieldElement.setAttribute("type", "date");
            case TIMESTAMP -> fieldElement.setAttribute("type", "date");
            case ARRAY -> setSchemaField(document, fields, name, type.getArrayElementType(), nullable, true);
            default -> {
                return;
            }
        }
        fields.appendChild(fieldElement);
    }

}
