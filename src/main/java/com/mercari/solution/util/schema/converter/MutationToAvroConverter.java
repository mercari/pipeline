package com.mercari.solution.util.schema.converter;

import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Value;
import com.mercari.solution.util.DateTimeUtil;
import com.mercari.solution.util.schema.StructSchemaUtil;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.beam.sdk.io.gcp.bigquery.AvroWriteRequest;
import org.joda.time.Instant;

import java.nio.ByteBuffer;

public class MutationToAvroConverter {

    public static GenericRecord convert(final AvroWriteRequest<Mutation> writeRequest) {
        final Schema schema = writeRequest.getSchema();
        final Mutation mutation = writeRequest.getElement();
        return convert(schema, mutation);
    }

    public static GenericRecord convertMutationRecord(final AvroWriteRequest<Mutation> writeRequest) {
        final Schema schema = writeRequest.getSchema();
        final Mutation mutation = writeRequest.getElement();
        return convertMutationRecord(schema, mutation);
    }

    public static GenericRecord convert(final Schema schema, final Mutation mutation) {
        final GenericRecordBuilder builder = new GenericRecordBuilder(schema);
        for(final Schema.Field field : schema.getFields()) {
            final Object value = convertRecordValue(mutation.asMap().get(field.name()));
            builder.set(field, value);
        }
        return builder.build();
    }

    public static GenericRecord convertMutationRecord(final Schema schema, final Mutation mutation) {
        return new GenericRecordBuilder(schema)
                .set("table", mutation.getTable())
                .set("op", mutation.getOperation())
                .set("timestamp", Instant.now().getMillis() * 1000L)
                .set("keys", StructSchemaUtil.getKeys(mutation))
                .set("mutation", MutationToJsonConverter.convertJsonString(mutation))
                .build();
    }

    private static Object convertRecordValue(final Value value) {
        if(value == null) {
            return null;
        }
        return switch (value.getType().getCode()) {
            case STRING -> value.getString();
            case BOOL -> value.getBool();
            case INT64 -> value.getInt64();
            case FLOAT64 -> value.getFloat64();
            case DATE -> DateTimeUtil.toEpochDay(value.getDate());
            case TIMESTAMP -> DateTimeUtil.toEpochMicroSecond(value.getTimestamp());
            case JSON -> value.getJson();
            case BYTES -> ByteBuffer.wrap(value.getBytes().toByteArray());
            case NUMERIC -> value.getNumeric().toString();
            case STRUCT -> {
                final Struct struct = value.getStruct();
                yield StructToAvroConverter.convert(StructToAvroConverter.convertSchema(struct.getType()), struct);
            }
            default -> throw new IllegalArgumentException();
        };
    }

}
