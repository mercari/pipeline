package com.mercari.solution.config.options;

import org.apache.beam.sdk.options.PipelineOptions;

import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;

public class FlinkOptions implements Serializable {

    private String flinkMaster;
    private Integer parallelism;
    private Integer maxParallelism;
    private Boolean allowNonRestoredState;
    private Boolean attachedMode;
    private String executionModeForBatch;

    public static void setOptions(
            final PipelineOptions pipelineOptions,
            final FlinkOptions flink) {

        if(flink == null) {
            return;
        }

        try {
            final Class<? extends PipelineOptions> clazz = (Class<? extends PipelineOptions>)Class.forName("org.apache.beam.runners.flink.FlinkPipelineOptions");
            final PipelineOptions flinkOptions = pipelineOptions.as(clazz);

            if(flink.flinkMaster != null) {
                clazz.getMethod("setFlinkMaster", String.class).invoke(flinkOptions, flink.flinkMaster);
            }
            if(flink.parallelism != null) {
                clazz.getMethod("setParallelism", Integer.class).invoke(flinkOptions, flink.parallelism);
            }
            if(flink.maxParallelism != null) {
                clazz.getMethod("setMaxParallelism", Integer.class).invoke(flinkOptions, flink.maxParallelism);
            }
            if(flink.allowNonRestoredState != null) {
                clazz.getMethod("setAllowNonRestoredState", boolean.class).invoke(flinkOptions, flink.allowNonRestoredState);
            }
            if(flink.attachedMode != null) {
                clazz.getMethod("setAttachedMode", boolean.class).invoke(flinkOptions, flink.attachedMode);
            }
            if(flink.executionModeForBatch != null) {
                clazz.getMethod("setExecutionModeForBatch", String.class).invoke(flinkOptions, flink.executionModeForBatch);
            }

        } catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
            throw new RuntimeException("Failed to set flink runner pipeline options", e);
        }
    }
}
