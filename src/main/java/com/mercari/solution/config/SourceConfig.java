package com.mercari.solution.config;

import com.google.gson.JsonObject;
import com.mercari.solution.module.Schema;
import com.mercari.solution.module.Source;

public class SourceConfig extends ModuleConfig {

    public static final String OPTION_ORIGINAL_FIELD_NAME = "originalFieldName";

    private JsonObject schema;
    private String timestampAttribute;
    private String timestampDefault;

    private Source.Mode mode;

    public Schema getSchema() {
        return Schema.parse(schema);
    }

    public String getTimestampAttribute() {
        return timestampAttribute;
    }

    public String getTimestampDefault() {
        if(timestampDefault == null) {
            return "1970-01-01T00:00:00Z";
        }
        return timestampDefault;
    }

    public Source.Mode getMode() {
        return mode;
    }

}

