package com.mercari.solution.config.options;

import org.apache.beam.sdk.options.PipelineOptions;

import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.util.List;

public class PortableOptions implements Serializable {

    private String defaultEnvironmentConfig;
    private String defaultEnvironmentType;
    private Boolean enableHeapDumps;
    private Integer environmentCacheMillis;
    private Integer environmentExpirationMillis;
    private List<String> environmentOptions;
    private String jobEndpoint;
    private Integer jobServerTimeout;
    private Boolean loadBalanceBundles;
    private String outputExecutablePath;
    private Integer sdkWorkerParallelism;

    public static void setOptions(
            final PipelineOptions pipelineOptions,
            final PortableOptions portable) {

        if(portable == null) {
            return;
        }

        try {
            final Class<? extends PipelineOptions> clazz = (Class<? extends PipelineOptions>)Class.forName("org.apache.beam.runners.portable.PortablePipelineOptions");
            final PipelineOptions portableOptions = pipelineOptions.as(clazz);

            if(portable.defaultEnvironmentConfig != null) {
                clazz.getMethod("setDefaultEnvironmentConfig", String.class).invoke(portableOptions, portable.defaultEnvironmentConfig);
            }
            if(portable.defaultEnvironmentType != null) {
                clazz.getMethod("setDefaultEnvironmentType", String.class).invoke(portableOptions, portable.defaultEnvironmentType);
            }
            if(portable.enableHeapDumps != null) {
                clazz.getMethod("setEnableHeapDumps", boolean.class).invoke(portableOptions, portable.enableHeapDumps);
            }
            if(portable.environmentCacheMillis != null) {
                clazz.getMethod("setEnvironmentExpirationMillis", int.class).invoke(portableOptions, portable.environmentCacheMillis);
            }
            if(portable.environmentExpirationMillis != null) {
                clazz.getMethod("setEnvironmentCacheMillis", int.class).invoke(portableOptions, portable.environmentExpirationMillis);
            }
            if(portable.environmentOptions != null) {
                clazz.getMethod("setEnvironmentOptions", List.class).invoke(portableOptions, portable.environmentOptions);
            }
            if(portable.jobEndpoint != null) {
                clazz.getMethod("setJobEndpoint", String.class).invoke(portableOptions, portable.jobEndpoint);
            }
            if(portable.jobServerTimeout != null) {
                clazz.getMethod("setJobServerTimeout", int.class).invoke(portableOptions, portable.jobServerTimeout);
            }
            if(portable.loadBalanceBundles != null) {
                clazz.getMethod("setLoadBalanceBundles", boolean.class).invoke(portableOptions, portable.loadBalanceBundles);
            }
            if(portable.outputExecutablePath != null) {
                clazz.getMethod("setOutputExecutablePath", String.class).invoke(portableOptions, portable.outputExecutablePath);
            }
            if(portable.sdkWorkerParallelism != null) {
                clazz.getMethod("setSdkWorkerParallelism", int.class).invoke(portableOptions, portable.sdkWorkerParallelism);
            }
        } catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
            throw new RuntimeException("Failed to set portable runner pipeline options", e);
        }
    }
}
