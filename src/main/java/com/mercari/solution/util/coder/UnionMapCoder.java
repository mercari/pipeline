package com.mercari.solution.util.coder;

import org.apache.avro.Schema;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.MapCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.extensions.avro.coders.AvroGenericCoder;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class UnionMapCoder {

    public static Coder<Map<String,Object>> mapCoder() {
        return mapCoder(6);
    }

    public static Coder<Map<String,Object>> mapCoder(int depth) {
        return MapCoder.of(StringUtf8Coder.of(), unionValueCoder(depth));
    }

    public static Coder<Object> unionValueCoder() {
        return unionValueCoder(6);
    }

    public static Coder<Object> unionValueCoder(int depth) {
        return AvroGenericCoder.of(Object.class, unionValueSchema(depth), false);
    }

    public static Coder<Object> primitiveUnionValueCoder() {
        return AvroGenericCoder.of(Object.class, primitiveUnionValueSchema(), false);
    }

    public static int serializeSize(Map<String,Object> value) {
        try(ByteArrayOutputStream os = new ByteArrayOutputStream()) {
            mapCoder().encode(value, os);
            byte[] b = os.toByteArray();
            return b.length;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static org.apache.avro.Schema mapSchema(int depth) {
        final org.apache.avro.Schema unionValueSchema = unionValueSchema(depth);
        return org.apache.avro.Schema.createMap(unionValueSchema);
    }

    private static org.apache.avro.Schema unionValueSchema(int depth) {
        final List<Schema> unionSchema = new ArrayList<>();
        unionSchema.add(org.apache.avro.Schema.create(org.apache.avro.Schema.Type.STRING));
        unionSchema.add(org.apache.avro.Schema.create(org.apache.avro.Schema.Type.LONG));
        unionSchema.add(org.apache.avro.Schema.create(org.apache.avro.Schema.Type.NULL));
        unionSchema.add(org.apache.avro.Schema.create(org.apache.avro.Schema.Type.INT));
        unionSchema.add(org.apache.avro.Schema.create(org.apache.avro.Schema.Type.DOUBLE));
        unionSchema.add(org.apache.avro.Schema.create(org.apache.avro.Schema.Type.FLOAT));
        unionSchema.add(org.apache.avro.Schema.create(org.apache.avro.Schema.Type.BOOLEAN));
        unionSchema.add(org.apache.avro.Schema.create(org.apache.avro.Schema.Type.BYTES));

        unionSchema.add(org.apache.avro.Schema.createArray(arrayElementSchema(depth)));

        if(depth > 0) {
            final org.apache.avro.Schema nestedSchema = mapSchema(depth - 1);
            unionSchema.add(nestedSchema);
        }

        return org.apache.avro.Schema.createUnion(unionSchema);
    }

    // without nested array
    private static org.apache.avro.Schema arrayElementSchema(int depth) {
        final List<org.apache.avro.Schema> unionSchema = new ArrayList<>();
        unionSchema.add(org.apache.avro.Schema.create(org.apache.avro.Schema.Type.STRING));
        unionSchema.add(org.apache.avro.Schema.create(org.apache.avro.Schema.Type.DOUBLE));
        unionSchema.add(org.apache.avro.Schema.create(org.apache.avro.Schema.Type.LONG));
        unionSchema.add(org.apache.avro.Schema.create(org.apache.avro.Schema.Type.INT));
        unionSchema.add(org.apache.avro.Schema.create(org.apache.avro.Schema.Type.NULL));
        unionSchema.add(org.apache.avro.Schema.create(org.apache.avro.Schema.Type.FLOAT));
        unionSchema.add(org.apache.avro.Schema.create(org.apache.avro.Schema.Type.BOOLEAN));
        unionSchema.add(org.apache.avro.Schema.create(org.apache.avro.Schema.Type.BYTES));

        if(depth > 0) {
            final org.apache.avro.Schema nestedSchema = mapSchema(depth - 1);
            unionSchema.add(nestedSchema);
        }

        return org.apache.avro.Schema.createUnion(unionSchema);
    }

    private static org.apache.avro.Schema primitiveUnionValueSchema() {
        final List<org.apache.avro.Schema> unionSchema = new ArrayList<>();
        unionSchema.add(org.apache.avro.Schema.create(org.apache.avro.Schema.Type.STRING));
        unionSchema.add(org.apache.avro.Schema.create(org.apache.avro.Schema.Type.DOUBLE));
        unionSchema.add(org.apache.avro.Schema.create(org.apache.avro.Schema.Type.LONG));
        unionSchema.add(org.apache.avro.Schema.create(org.apache.avro.Schema.Type.INT));
        unionSchema.add(org.apache.avro.Schema.create(org.apache.avro.Schema.Type.NULL));
        unionSchema.add(org.apache.avro.Schema.create(org.apache.avro.Schema.Type.FLOAT));
        unionSchema.add(org.apache.avro.Schema.create(org.apache.avro.Schema.Type.BOOLEAN));
        unionSchema.add(org.apache.avro.Schema.create(org.apache.avro.Schema.Type.BYTES));
        return org.apache.avro.Schema.createUnion(unionSchema);
    }

}
