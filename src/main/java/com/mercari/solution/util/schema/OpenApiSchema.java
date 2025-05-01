package com.mercari.solution.util.schema;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.annotations.SerializedName;
import com.mercari.solution.module.Schema;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

public class OpenApiSchema implements Serializable {

    public Type type;
    public String format;
    public String title;
    public String description;
    public Boolean nullable;
    @SerializedName("default")
    public String defaultValue;
    public OpenApiSchema items;
    public Map<String, OpenApiSchema> properties;
    public List<String> propertyOrdering;
    public List<String> required;

    public JsonObject toJson() {
        final JsonObject schemaJson = new JsonObject();
        if(type != null) {
            schemaJson.addProperty("type", type.name());
        }
        if(format != null) {
            schemaJson.addProperty("format", format);
        }
        if(title != null) {
            schemaJson.addProperty("title", title);
        }
        if(description != null) {
            schemaJson.addProperty("description", description);
        }
        if(nullable != null) {
            schemaJson.addProperty("nullable", nullable);
        }
        if(items != null) {
            schemaJson.add("items", items.toJson());
        }
        if(properties != null) {
            final JsonObject propertiesJson = new JsonObject();
            for(final Map.Entry<String, OpenApiSchema> entry : properties.entrySet()) {
                propertiesJson.add(entry.getKey(), entry.getValue().toJson());
            }
            schemaJson.add("properties", propertiesJson);
        }
        if(propertyOrdering != null) {
            final JsonArray propertyOrderingArray = new JsonArray();
            for(final String property : propertyOrdering) {
                propertyOrderingArray.add(property);
            }
            schemaJson.add("propertyOrdering", propertyOrderingArray);
        }
        if(required != null) {
            final JsonArray requiredArray = new JsonArray();
            for(final String property : required) {
                requiredArray.add(property);
            }
            schemaJson.add("required", requiredArray);
        }

        return schemaJson;
    }

    public static Schema createSchema() {
        return createSchema(3);
    }

    public static Schema createSchema(int depth) {
        final Schema.Builder builder = Schema.builder()
                .withField("type", Schema.FieldType.STRING)
                .withField("format", Schema.FieldType.STRING)
                .withField("title", Schema.FieldType.STRING)
                .withField("description", Schema.FieldType.STRING)
                .withField("nullable", Schema.FieldType.BOOLEAN)
                .withField("default", Schema.FieldType.STRING)
                .withField("propertyOrdering", Schema.FieldType.array(Schema.FieldType.STRING))
                .withField("required", Schema.FieldType.array(Schema.FieldType.STRING))
                .withField("minimum", Schema.FieldType.FLOAT64)
                .withField("maximum", Schema.FieldType.FLOAT64)
                .withField("minLength", Schema.FieldType.STRING)
                .withField("maxLength", Schema.FieldType.STRING)
                .withField("pattern", Schema.FieldType.STRING);
        if(depth < 0) {
            return builder.build();
        }

        return builder
                .withField("items", Schema.FieldType.element(createSchema(depth-1)))
                .withField("properties", Schema.FieldType.map(Schema.FieldType.element(createSchema(depth-1))))
                .build();
    }

    public static Schema createSchema(final OpenApiSchema openApiSchema) {
        return Schema.builder()
                .withField("format", Schema.FieldType.STRING)
                .withField("title", Schema.FieldType.STRING)
                .withField("description", Schema.FieldType.STRING)
                .withField("nullable", Schema.FieldType.BOOLEAN)
                .withField("default", Schema.FieldType.STRING)
                //.withField("items", Schema.FieldType.array(Schema.FieldType.element(cre)))
                //.withField("properties", Schema.FieldType.map(Schema.FieldType.element(cre)))
                .withField("propertyOrdering", Schema.FieldType.array(Schema.FieldType.STRING))
                .withField("required", Schema.FieldType.array(Schema.FieldType.STRING))
                .build();
    }


    public enum Type {
        TYPE_UNSPECIFIED,
        STRING,
        NUMBER,
        INTEGER,
        BOOLEAN,
        ARRAY,
        OBJECT
    }

    public enum Format {

    }

}
