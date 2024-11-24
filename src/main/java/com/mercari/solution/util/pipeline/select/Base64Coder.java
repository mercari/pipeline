package com.mercari.solution.util.pipeline.select;

import com.google.gson.JsonObject;
import com.mercari.solution.module.Schema;
import com.mercari.solution.util.schema.ElementSchemaUtil;
import org.joda.time.Instant;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;

public class Base64Coder implements SelectFunction {

    private final String name;
    private final String field;
    private final List<Schema.Field> inputFields;
    private final Schema.FieldType outputFieldType;

    private final Code code;

    private final boolean ignore;

    public enum Code {
        decode,
        encode;
    }

    Base64Coder(
            String name, String field, Schema.FieldType outputFieldType, List<Schema.Field> inputFields, Code code, boolean ignore) {
        this.name = name;
        this.field = field;
        this.inputFields = inputFields;
        this.outputFieldType = outputFieldType;
        this.code = code;
        this.ignore = ignore;
    }

    public static Base64Coder of(String name, JsonObject jsonObject, List<Schema.Field> inputFields, boolean encode, boolean ignore) {
        final Code code = encode ? Code.encode : Code.decode;
        if(!jsonObject.has("type")) {
            throw new IllegalArgumentException("SelectField base64" + code + ": " + name + " requires type parameter");
        }
        final String type = jsonObject.get("type").getAsString();

        final String field;
        if(jsonObject.has("field")) {
            if(!jsonObject.get("field").isJsonPrimitive()) {
                throw new IllegalArgumentException("SelectField base64" + code.name() + ": " + name + ".field parameter must be string");
            }
            field = jsonObject.get("field").getAsString();
        } else {
            field = name;
        }

        final List<Schema.Field> fields = new ArrayList<>();
        final Schema.FieldType inputFieldType = ElementSchemaUtil.getInputFieldType(field, inputFields);
        if(inputFieldType == null) {
            throw new IllegalArgumentException("SelectField base64" + code + ": " + name + " missing inputField: " + field);
        }
        fields.add(Schema.Field.of(field, inputFieldType));

        final Schema.FieldType outputFieldType = Schema.FieldType.type(Schema.Type.of(type));
        return new Base64Coder(name, field, outputFieldType, fields, code, ignore);
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public boolean ignore() {
        return ignore;
    }

    @Override
    public List<Schema.Field> getInputFields() {
        return inputFields;
    }

    @Override
    public Schema.FieldType getOutputFieldType() {
        return outputFieldType;
    }

    @Override
    public void setup() {

    }

    @Override
    public Object apply(Map<String, Object> input, Instant timestamp) {
        final Object value = ElementSchemaUtil.getValue(input, field);
        return switch (code) {
            case encode -> {
                final byte[] bytes = switch (value) {
                    case byte[] b -> b;
                    case ByteBuffer byteBuffer -> byteBuffer.array();
                    case String s -> s.getBytes(StandardCharsets.UTF_8);
                    case Number n -> n.toString().getBytes(StandardCharsets.UTF_8);
                    case null, default -> null;
                };
                if(bytes == null) {
                    yield null;
                }
                yield switch (outputFieldType.getType()) {
                    case bytes -> ByteBuffer.wrap(Base64.getEncoder().encode(bytes));
                    case string, json -> Base64.getEncoder().encodeToString(bytes);
                    default -> new IllegalArgumentException();
                };
            }
            case decode -> {
                byte[] bytes = switch (value) {
                    case String s -> Base64.getDecoder().decode(s);
                    case ByteBuffer b -> Base64.getDecoder().decode(b).array();
                    case byte[] b -> Base64.getDecoder().decode(b);
                    case null, default -> null;
                };
                if(bytes == null) {
                    yield null;
                }
                yield switch (outputFieldType.getType()) {
                    case bytes -> ByteBuffer.wrap(bytes);
                    case string, json -> new String(bytes, StandardCharsets.UTF_8);
                    default -> new IllegalArgumentException();
                };
            }
        };
    }

}
