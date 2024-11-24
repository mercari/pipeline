package com.mercari.solution.util.schema.converter;

import com.google.bigtable.v2.Mutation;
import com.mercari.solution.module.MElement;
import com.mercari.solution.module.Schema;
import com.mercari.solution.util.schema.BigtableSchemaUtil;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.values.Row;

import java.util.Map;

public class ElementToBigtableConverter {

    public static Iterable<Mutation> convert(
            final Schema schema,
            final MElement element,
            final String defaultColumnFamily,
            final BigtableSchemaUtil.Format defaultFormat,
            final BigtableSchemaUtil.MutationOp defaultMutationOp,
            final Map<String, BigtableSchemaUtil.ColumnSetting> columnSettings,
            final long timestampMicros) {

        if(element == null || element.getValue() == null) {
            return null;
        }
        return switch (element.getType()) {
            case ELEMENT -> convert(schema, (Map<String,Object>)element.getValue(), defaultColumnFamily, defaultFormat, defaultMutationOp, columnSettings, timestampMicros);
            case AVRO -> AvroToBigtableConverter.convert(schema.getAvroSchema(), (GenericRecord) element.getValue(), defaultColumnFamily, defaultFormat, defaultMutationOp, columnSettings, timestampMicros);
            case ROW -> RowToBigtableConverter.convert(schema.getRowSchema(), (Row) element.getValue(), defaultColumnFamily, defaultFormat, defaultMutationOp, columnSettings, timestampMicros);
            //case STRUCT -> StructToBigtableConverter.convert(schema.getS, (GenericRecord) element.getValue();
            //case DOCUMENT -> (GenericRecord) element.getValue();
            //case ENTITY -> (GenericRecord) element.getValue();
            default -> throw new IllegalArgumentException();
        };
    }

    public static Iterable<Mutation> convert(
            final Schema schema,
            final Map<String, Object> values,
            final String defaultColumnFamily,
            final BigtableSchemaUtil.Format defaultFormat,
            final BigtableSchemaUtil.MutationOp defaultMutationOp,
            final Map<String, BigtableSchemaUtil.ColumnSetting> columnSettings,
            final long timestampMicros) {

        // TODO
        throw new IllegalArgumentException("Not implemented");
    }

}
