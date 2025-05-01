package com.mercari.solution.config;

import com.google.gson.JsonObject;
import com.mercari.solution.module.Logging;
import com.mercari.solution.module.Strategy;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class FailureConfig implements Serializable {

    private String name;
    private String module;
    private JsonObject parameters;

    private Set<String> tags;
    private List<Logging> loggings;

    private Boolean ignore;

    private Map<String, String> args;

    private Strategy strategy;

    public String getName() {
        return name;
    }

    public String getModule() {
        return module;
    }

    public JsonObject getParameters() {
        return parameters;
    }

    public Set<String> getTags() {
        return tags;
    }

    public List<Logging> getLoggings() {
        return loggings;
    }

    public Boolean getIgnore() {
        return ignore;
    }

    public Map<String, String> getArgs() {
        return args;
    }

    public void applyContext(final String context) {
        if(context == null || context.isEmpty()) {
            return;
        }
        if(this.tags == null || this.tags.isEmpty()) {
            this.ignore = true;
        } else {
            this.ignore = !tags.contains(context);
        }
    }

    public void setArgs(Map<String, String> args) {
        this.args = args;
    }

    public Strategy getStrategy() {
        return strategy;
    }

}
