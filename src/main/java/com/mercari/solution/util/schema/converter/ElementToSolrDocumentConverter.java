package com.mercari.solution.util.schema.converter;

/*
import com.mercari.solution.module.Schema;
import com.mercari.solution.util.DateTimeUtil;
import org.apache.solr.common.SolrInputDocument;

import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.util.*;
 */

public class ElementToSolrDocumentConverter {

    /*
    public static SolrInputDocument convert(final Schema schema, final Map<String, Object> values, final List<String> fieldNames) {
        final SolrInputDocument doc = new SolrInputDocument();
        for(final Schema.Field field : schema.getFields()) {
            if(fieldNames != null && !fieldNames.contains(field.getName())) {
                continue;
            }
            final Object value = values.get(field.getName());
            if(Schema.Type.element.equals(field.getFieldType().getType())) {
                final Map<String, Object> childValues = (Map<String, Object>) value;
                final SolrInputDocument childDoc = convert(field.getFieldType().getElementSchema(), childValues, null);
                doc.addChildDocument(childDoc);
            } else if(Schema.Type.array.equals(field.getFieldType().getType())) {
                if(value != null) {
                    final List<?> list = (List<?>) value;
                    for(final Object child : list) {
                        final Object docValue = getFieldValue(field.getFieldType().getArrayValueType(), child, fieldNames);
                        doc.addField(field.getName(), docValue);
                    }
                }
            } else {
                final Object docValue = getFieldValue(field.getFieldType(), value, fieldNames);
                doc.addField(field.getName(),docValue);
            }
        }
        return doc;
    }

    private static Object getFieldValue(
            final Schema.FieldType fieldType,
            final Object value,
            final List<String> fieldNames) {

        return switch (fieldType.getType()) {
            case bool -> Optional.ofNullable(value).orElse(false);
            case string, json -> Optional.ofNullable(value).orElse("");
            case bytes -> Optional.ofNullable(value).map(o -> ((ByteBuffer)o).array()).orElse(new byte[0]);
            case int16, int32 -> Optional.ofNullable(value).orElse(0);
            case int64 -> Optional.ofNullable(value).orElse(0L);
            case float16, float32 -> Optional.ofNullable(value).orElse(0F);
            case float64 -> Optional.ofNullable(value).orElse(0D);
            case timestamp -> Optional.ofNullable(value).map(o -> java.util.Date.from(DateTimeUtil.toInstant((Long) o))).orElse(new Date(0));
            case date -> Optional.ofNullable(value).map(o -> java.util.Date.from(DateTimeUtil.toInstant(LocalDate.ofEpochDay(((Integer) o).longValue()).toString()))).orElse(new Date(0));
            default -> throw new IllegalArgumentException();
        };
    }
     */

}
