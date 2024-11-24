package com.mercari.solution.util.schema.converter;

import com.google.datastore.v1.ArrayValue;
import com.google.datastore.v1.Entity;
import com.google.datastore.v1.Value;
import com.mercari.solution.util.schema.EntitySchemaUtil;
import org.apache.solr.common.SolrInputDocument;

import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.Objects;


public class EntityToSolrDocumentConverter {

    public static SolrInputDocument convert(final Entity entity, final List<String> fieldNames) {
        return convert(entity, null, fieldNames);
    }

    public static SolrInputDocument convert(final Entity entity, final String parentName, final List<String> fieldNames) {
        final SolrInputDocument doc = new SolrInputDocument();
        for(final Map.Entry<String, Value> entry : entity.getPropertiesMap().entrySet()) {
            if(fieldNames != null && !fieldNames.contains(entry.getKey())) {
                continue;
            }
            setFieldValue(doc, parentName, entry.getKey(), entry.getValue(), fieldNames);
        }
        return doc;
    }

    private static void setFieldValue(final SolrInputDocument doc,
                                      final String parentName, final String fieldName,
                                      final Value value,
                                      final List<String> fieldNames) {

        final boolean isNullField = value == null
                || value.getValueTypeCase().equals(Value.ValueTypeCase.NULL_VALUE)
                || value.getValueTypeCase().equals(Value.ValueTypeCase.VALUETYPE_NOT_SET);
        final String name = parentName == null ? fieldName : parentName + "." + fieldName;
        if(isNullField) {
            doc.addField(name, "");
            return;
        }
        switch (value.getValueTypeCase()) {
            case BOOLEAN_VALUE: {
                doc.addField(name, value.getBooleanValue());
                return;
            }
            case STRING_VALUE: {
                doc.addField(name, value.getStringValue());
                return;
            }
            case BLOB_VALUE: {
                doc.addField(name, value.getBlobValue().toByteArray());
                return;
            }
            case TIMESTAMP_VALUE: {
                final com.google.cloud.Date date = EntitySchemaUtil.convertDate(value);
                final LocalDate ld = LocalDate.of(date.getYear(), date.getMonth(), date.getDayOfMonth());
                doc.addField(name, java.util.Date.from(ld.atStartOfDay().toInstant(ZoneOffset.UTC)));
                return;
            }
            case INTEGER_VALUE: {
                doc.addField(name, value.getIntegerValue());
                return;
            }
            case DOUBLE_VALUE: {
                doc.addField(name, value.getDoubleValue());
                return;
            }
            case ENTITY_VALUE: {
                doc.addChildDocument(convert(value.getEntityValue(), name, fieldNames));
                return;
            }
            case ARRAY_VALUE: {
                setArrayFieldValue(doc, parentName, name, value.getArrayValue(), fieldNames);
                return;
            }
            case NULL_VALUE:
            case VALUETYPE_NOT_SET: {
                return;
            }
            case KEY_VALUE:
            case GEO_POINT_VALUE:
            default:
                return;
        }
    }

    private static void setArrayFieldValue(final SolrInputDocument doc,
                                           final String parentName, final String fieldName,
                                           final ArrayValue arrayValue,
                                           final List<String> fieldNames) {

        if(arrayValue == null || arrayValue.getValuesCount() == 0) {
            return;
        }
        final List<Value> values = arrayValue.getValuesList();
        final String name = parentName == null ? fieldName : parentName + "." + fieldName;

        values.stream()
                .filter(Objects::nonNull)
                .forEach(v -> {
                    switch (v.getValueTypeCase()) {
                        case BOOLEAN_VALUE: {
                            doc.addField(name, v.getBooleanValue());
                            break;
                        }
                        case STRING_VALUE: {
                            doc.addField(name, v.getStringValue());
                            break;
                        }
                        case INTEGER_VALUE: {
                            doc.addField(name, v.getIntegerValue());
                            break;
                        }
                        case DOUBLE_VALUE: {
                            doc.addField(name, v.getDoubleValue());
                            break;
                        }
                        case BLOB_VALUE: {
                            doc.addField(name, v.getBlobValue().toByteArray());
                            break;
                        }
                        case TIMESTAMP_VALUE: {
                            final com.google.cloud.Date date = EntitySchemaUtil.convertDate(v);
                            final LocalDate ld = LocalDate.of(date.getYear(), date.getMonth(), date.getDayOfMonth());
                            doc.addField(name, java.util.Date.from(ld.atStartOfDay().toInstant(ZoneOffset.UTC)));
                            break;
                        }
                        case ENTITY_VALUE: {
                            doc.addChildDocument(convert(v.getEntityValue(), name, fieldNames));
                            break;
                        }
                        case GEO_POINT_VALUE:
                        case KEY_VALUE:
                        case NULL_VALUE:
                        case VALUETYPE_NOT_SET:
                        case ARRAY_VALUE:
                        default:
                            return;
                    }
                });


    }

}
