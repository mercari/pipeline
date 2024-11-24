package com.mercari.solution.util.schema.converter;

import com.google.bigtable.v2.Mutation;
import com.google.protobuf.*;
import com.mercari.solution.util.schema.AvroSchemaUtil;
import com.mercari.solution.util.schema.BigtableSchemaUtil;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;


public class AvroToBigtableConverter {

    public static Iterable<Mutation> convert(final Schema schema,
                                             final GenericRecord record,
                                             final String defaultColumnFamily,
                                             final BigtableSchemaUtil.Format defaultFormat,
                                             final BigtableSchemaUtil.MutationOp defaultMutationOp,
                                             final Map<String, BigtableSchemaUtil.ColumnSetting> columnSettings,
                                             final long timestampMicros) {

        final List<Mutation> mutations = new ArrayList<>();

        if(BigtableSchemaUtil.MutationOp.DELETE_FROM_ROW.equals(defaultMutationOp)) {
            final Mutation.DeleteFromRow deleteFromRow = Mutation.DeleteFromRow.newBuilder().build();
            final Mutation mutation = Mutation.newBuilder().setDeleteFromRow(deleteFromRow).build();
            mutations.add(mutation);
            return mutations;
        } else if(BigtableSchemaUtil.MutationOp.DELETE_FROM_FAMILY.equals(defaultMutationOp)) {
            final Set<String> columnFamilies = new HashSet<>();
            columnFamilies.add(defaultColumnFamily);
            for(final Schema.Field field : schema.getFields()) {
                final BigtableSchemaUtil.ColumnSetting columnSetting = columnSettings.getOrDefault(field.name(), null);
                if(columnSetting == null || columnSetting.getColumnFamily() == null) {
                    continue;
                }
                if(BigtableSchemaUtil.MutationOp.DELETE_FROM_FAMILY.equals(columnSetting.getMutationOp())) {
                    columnFamilies.add(columnSetting.getColumnFamily());
                }
            }

            for(final String columnFamily : columnFamilies) {
                final Mutation.DeleteFromFamily deleteFromFamily = Mutation.DeleteFromFamily.newBuilder()
                        .setFamilyName(columnFamily)
                        .build();
                final Mutation mutation = Mutation.newBuilder().setDeleteFromFamily(deleteFromFamily).build();
                mutations.add(mutation);
            }
            return mutations;
        }

        for(final Schema.Field field : schema.getFields()) {
            final BigtableSchemaUtil.ColumnSetting columnSetting = columnSettings.getOrDefault(field.name(), null);

            final String columnFamily;
            final String columnQualifier;
            final BigtableSchemaUtil.Format format;
            final BigtableSchemaUtil.MutationOp mutationOp;

            if(columnSetting == null) {
                columnFamily = defaultColumnFamily;
                columnQualifier = field.name();
                format = defaultFormat;
                mutationOp = defaultMutationOp;
            } else {
                if(columnSetting.getExclude() != null && columnSetting.getExclude()) {
                    continue;
                }
                columnFamily = columnSetting.getColumnFamily();
                columnQualifier = columnSetting.getColumnQualifier();
                format = columnSetting.getFormat();
                mutationOp = columnSetting.getMutationOp();
            }

            if(BigtableSchemaUtil.MutationOp.DELETE_FROM_COLUMN.equals(mutationOp)) {
                final Mutation.DeleteFromColumn deleteFromColumn = Mutation.DeleteFromColumn.newBuilder()
                        .setFamilyName(columnFamily)
                        .setColumnQualifier(ByteString.copyFrom(columnQualifier, StandardCharsets.UTF_8))
                        .build();
                final Mutation mutation = Mutation.newBuilder().setDeleteFromColumn(deleteFromColumn).build();
                mutations.add(mutation);
            } else {

                final Object value = record.hasField(field.name()) ? record.get(field.name()) : null;
                if(value == null) {
                    continue;
                }

                final ByteString bytes = switch (format) {
                    case bytes -> AvroSchemaUtil.getAsByteString(record, field.name());
                    case string -> {
                        final String stringValue = AvroSchemaUtil.getAsString(record, field.name());
                        if(stringValue == null) {
                            yield null;
                        } else {
                            yield ByteString.copyFrom(stringValue, StandardCharsets.UTF_8);
                        }
                    }
                    case avro -> {
                        if(AvroSchemaUtil.unnestUnion(field.schema()).getType().equals(Schema.Type.RECORD)) {
                            final GenericRecord fieldRecord = (GenericRecord) value;
                            try {
                                yield ByteString.copyFrom(AvroSchemaUtil.encode(fieldRecord));
                            } catch (IOException e) {
                                throw new IllegalStateException(e);
                            }
                        } else {
                            yield AvroSchemaUtil.getAsByteString(record, field.name());
                        }
                    }
                    default -> throw new IllegalStateException();
                };

                if(bytes == null) {
                    continue;
                }

                final Mutation.SetCell cell = Mutation.SetCell.newBuilder()
                        .setFamilyName(columnFamily)
                        .setColumnQualifier(ByteString.copyFrom(columnQualifier, StandardCharsets.UTF_8))
                        .setValue(bytes)
                        .setTimestampMicros(timestampMicros)
                        .build();
                final Mutation mutation = Mutation.newBuilder().setSetCell(cell).build();
                mutations.add(mutation);
            }
        }

        return mutations;
    }

}
