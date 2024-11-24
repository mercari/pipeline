package com.mercari.solution.util.schema.converter;

import com.google.bigtable.v2.Cell;
import com.google.bigtable.v2.Column;
import com.google.bigtable.v2.Family;
import com.google.bigtable.v2.Row;
import com.google.protobuf.ByteString;
import com.mercari.solution.module.Schema;
import com.mercari.solution.util.schema.BigtableSchemaUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BigtableRowToElementConverter {

    private static final String KEY_FIELD_NAME = "__key__";

    private static final Logger LOG = LoggerFactory.getLogger(BigtableRowToElementConverter.class);

    public static Map<String, Object> convert(final Schema schema, final Row row) {
        if(schema == null) {
            throw new RuntimeException("schema must not be null for row: " + row);
        }
        final Map<String, Object> values = new HashMap<>();

        final Schema.Field keyField = schema.getField(KEY_FIELD_NAME);
        if(keyField != null) {
            switch (keyField.getFieldType().getType()) {
                case string -> values.put(KEY_FIELD_NAME, row.getKey().toStringUtf8());
                case bytes -> values.put(KEY_FIELD_NAME, ByteBuffer.wrap(row.getKey().toByteArray()));
            }
        }

        for(final Family family : row.getFamiliesList()) {
            final String familyName = family.getName();
            final Schema.Field familyField = schema.getField(familyName);
            if(familyField != null) {
                final Schema.FieldType familyFieldType = familyField.getFieldType();
                if(!Schema.Type.element.equals(familyFieldType.getType())) {
                    continue;
                }
                final Map<String, Object> familyMap = new HashMap<>();
                setBuilder(familyFieldType.getElementSchema(), familyMap, family.getColumnsList());
                values.put(familyName, familyMap);
            } else {
                setBuilder(schema, values, family.getColumnsList());
            }
        }

        return values;
    }

    private static void setBuilder(final Schema schema, final Map<String, Object> builder, final List<Column> columns) {
        for(final Column column : columns) {
            final ByteString qualifier = column.getQualifier();
            final String columnName = qualifier.toStringUtf8();
            final Schema.Field columnField = schema.getField(columnName);
            if(columnField == null) {
                continue;
            }
            final Schema.FieldType columnFieldType = columnField.getFieldType();
            if(Schema.Type.array.equals(columnFieldType.getType())) {
                final List<Object> list = new ArrayList<>();
                for(final Cell cell : column.getCellsList()) {
                    final Object value = getBytesValue(columnFieldType, cell.getValue());
                    list.add(value);
                }
                builder.put(columnName, list);
            } else {
                if(column.getCellsCount() > 0) {
                    final Object value = getBytesValue(columnFieldType, column.getCells(0).getValue());
                    builder.put(columnName, value);
                }
            }
        }
    }

    private static Object getBytesValue(Schema.FieldType fieldType, final ByteString value) {
        return BigtableSchemaUtil.toPrimitiveValueFromWritable(fieldType, value);
    }

}
