package com.mercari.solution.config;

import com.google.gson.JsonObject;
import com.mercari.solution.module.Schema;
import com.mercari.solution.module.Strategy;
import com.mercari.solution.util.pipeline.Union;

import java.util.ArrayList;
import java.util.List;

public class SinkConfig extends ModuleConfig {

    private String input;
    private List<String> inputs;
    private JsonObject schema;
    private Strategy strategy;
    private Union.Parameters union;

    public List<String> getInputs() {
        if(inputs != null && !inputs.isEmpty()) {
            return inputs;
        } else if(input != null) {
            final List<String> list = new ArrayList<>();
            list.add(input);
            return list;
        } else if("action".equalsIgnoreCase(getModule())) {
            return new ArrayList<>();
        } else {
            throw new IllegalArgumentException("Sink module: " + getName() + " has not input");
        }
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

    public Union.Parameters getUnion() {
        return union;
    }

}
