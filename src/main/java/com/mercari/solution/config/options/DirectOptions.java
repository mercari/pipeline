package com.mercari.solution.config.options;

import org.apache.beam.sdk.options.PipelineOptions;

import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;

public class DirectOptions implements Serializable {

    private Integer targetParallelism;
    private Boolean blockOnRun;
    private Boolean enforceImmutability;
    private Boolean enforceEncodability;

    public static void setOptions(
            final PipelineOptions pipelineOptions,
            final DirectOptions direct) {

        if(direct == null) {
            return;
        }

        try {
            final Class<? extends PipelineOptions> clazz = (Class<? extends PipelineOptions>)Class.forName("org.apache.beam.runners.direct.DirectOptions");
            final PipelineOptions directOptions = pipelineOptions.as(clazz);

            if(direct.targetParallelism != null) {
                clazz.getMethod("setTargetParallelism", int.class).invoke(directOptions, direct.targetParallelism);
            }
            if(direct.blockOnRun != null) {
                clazz.getMethod("setBlockOnRun", boolean.class).invoke(directOptions, direct.blockOnRun);
            }
            if(direct.enforceImmutability != null) {
                clazz.getMethod("setEnforceImmutability", boolean.class).invoke(directOptions, direct.enforceImmutability);
            }
            if(direct.enforceEncodability != null) {
                clazz.getMethod("setEnforceEncodability", boolean.class).invoke(directOptions, direct.enforceEncodability);
            }

        } catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
            throw new RuntimeException("Failed to set direct runner pipeline options", e);
        }
    }
}
