package com.mercari.solution.util.schema;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.annotations.SerializedName;

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
