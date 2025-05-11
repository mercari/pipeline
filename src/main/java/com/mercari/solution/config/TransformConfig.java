package com.mercari.solution.config;

import com.google.gson.JsonObject;
import com.mercari.solution.module.Schema;
import com.mercari.solution.module.Strategy;

import java.util.List;

public class TransformConfig extends ModuleConfig {

    private List<String> inputs;
    private JsonObject schema;
    private Strategy strategy;

    public List<String> getInputs() {
        return inputs;
    }

    public void setInputs(List<String> inputs) {
        this.inputs = inputs;
    }

    public Schema getSchema() {
        return Schema.parse(schema);
    }

    public Strategy getStrategy() {
        return strategy;
    }

}
