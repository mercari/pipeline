package com.mercari.solution.config.options;

import org.apache.beam.sdk.options.PipelineOptions;

import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;

public class PrismOptions implements Serializable {

    private Boolean enableWebUI;
    private String idleShutdownTimeout;
    private String prismLocation;
    private String prismVersionOverride;

    public static void setOptions(
            final PipelineOptions pipelineOptions,
            final PrismOptions prism) {

        if(prism == null) {
            return;
        }

        try {
            final Class<? extends PipelineOptions> clazz = (Class<? extends PipelineOptions>)Class.forName("org.apache.beam.runners.prism.PrismPipelineOptions");
            final PipelineOptions prismOptions = pipelineOptions.as(clazz);

            if(prism.enableWebUI != null) {
                clazz.getMethod("setEnableWebUI", boolean.class).invoke(prismOptions, prism.enableWebUI);
            }
            if(prism.idleShutdownTimeout != null) {
                clazz.getMethod("setIdleShutdownTimeout", String.class).invoke(prismOptions, prism.idleShutdownTimeout);
            }
            if(prism.prismLocation != null) {
                clazz.getMethod("setPrismLocation", String.class).invoke(prismOptions, prism.prismLocation);
            }
            if(prism.prismVersionOverride != null) {
                clazz.getMethod("setPrismVersionOverride", String.class).invoke(prismOptions, prism.prismVersionOverride);
            }

        } catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
            throw new RuntimeException("Failed to set prism runner pipeline options", e);
        }
    }
}

