package com.mercari.solution.config;

import com.google.gson.JsonObject;
import com.mercari.solution.module.DataType;
import com.mercari.solution.module.Logging;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

public class ModuleConfig implements Serializable {

    private String name;
    private String module;
    private JsonObject parameters;

    private List<String> waits;

    private List<Logging> loggings;

    private Boolean ignore;
    private Boolean failFast;
    private Boolean outputFailure;

    private DataType outputType;

    private String description;

    // template args
    private Map<String, Object> args;


    public String getName() {
        return name;
    }

    public String getModule() {
        return module;
    }

    public JsonObject getParameters() {
        return parameters;
    }

    public List<String> getWaits() {
        return waits;
    }

    public List<Logging> getLoggings() {
        return loggings;
    }

    public Boolean getIgnore() {
        return ignore;
    }

    public Boolean getFailFast() {
        return failFast;
    }

    public Boolean getOutputFailure() {
        return outputFailure;
    }

    public DataType getOutputType() {
        return outputType;
    }

    public String getDescription() {
        return description;
    }

    public Map<String, Object> getArgs() {
        return args;
    }

    public void setArgs(Map<String, Object> args) {
        this.args = args;
    }

}
