package com.mercari.solution.util.coder;

import org.apache.avro.Schema;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.ListCoder;
import org.apache.beam.sdk.coders.MapCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.extensions.avro.coders.AvroGenericCoder;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class UnionMapCoder {

    private static final int DEFAULT_DEPTH = 6;

    public static Coder<Map<String,Object>> mapCoder() {
        return mapCoder(DEFAULT_DEPTH);
    }

    public static Coder<Map<String,Object>> mapCoder(int depth) {
        return MapCoder.of(StringUtf8Coder.of(), unionValueCoder(depth));
    }

    public static Coder<List<Object>> listCoder() {
        return listCoder(DEFAULT_DEPTH);
    }

    public static Coder<List<Object>> listCoder(int depth) {
        return ListCoder.of(unionValueCoder(depth));
    }

    public static Coder<Object> unionValueCoder() {
        return unionValueCoder(DEFAULT_DEPTH);
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

    private static Schema mapSchema(int depth) {
        final Schema unionValueSchema = unionValueSchema(depth);
        return Schema.createMap(unionValueSchema);
    }

    private static Schema unionValueSchema(int depth) {
        final List<Schema> unionSchema = createUnionValueSchemas();
        unionSchema.add(Schema.createArray(arrayElementSchema(depth)));

        if(depth > 0) {
            final Schema nestedSchema = mapSchema(depth - 1);
            unionSchema.add(nestedSchema);
        }

        return Schema.createUnion(unionSchema);
    }

    // without nested array
    private static Schema arrayElementSchema(int depth) {
        final List<Schema> unionSchema = createUnionValueSchemas();

        if(depth > 0) {
            final Schema nestedSchema = mapSchema(depth - 1);
            unionSchema.add(nestedSchema);
        }

        return Schema.createUnion(unionSchema);
    }

    private static Schema primitiveUnionValueSchema() {
        final List<Schema> unionSchema = createUnionValueSchemas();
        return Schema.createUnion(unionSchema);
    }

    private static List<Schema> createUnionValueSchemas() {
        final List<Schema> unionSchema = new ArrayList<>();
        unionSchema.add(Schema.create(Schema.Type.BOOLEAN));
        unionSchema.add(Schema.create(Schema.Type.STRING));
        unionSchema.add(Schema.create(Schema.Type.BYTES));
        unionSchema.add(Schema.create(Schema.Type.INT));
        unionSchema.add(Schema.create(Schema.Type.LONG));
        unionSchema.add(Schema.create(Schema.Type.FLOAT));
        unionSchema.add(Schema.create(Schema.Type.DOUBLE));
        unionSchema.add(Schema.create(Schema.Type.NULL));
        return unionSchema;
    }

}
