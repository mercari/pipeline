package com.mercari.solution.config.options;

import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.values.PInput;

import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class DataflowOptions implements Serializable {

    private String tempLocation;
    private String stagingLocation;
    private Map<String, String> labels;
    private String autoscalingAlgorithm;
    private String flexRSGoal;
    private Integer numWorkers;
    private Integer maxNumWorkers;
    private String workerMachineType;
    private Integer diskSizeGb;
    private String workerDiskType;
    private String workerRegion;
    private String workerZone;
    private String serviceAccount;
    private String impersonateServiceAccount;
    private String network;
    private String subnetwork;
    private Boolean usePublicIps;
    private Integer numberOfWorkerHarnessThreads;
    private Integer workerCacheMb;
    private String createFromSnapshot;
    private String sdkContainerImage;
    private Boolean enableStreamingEngine;
    private Integer streamingSideInputCacheExpirationMillis;
    private Boolean update;
    private Map<String, String> transformNameMapping;
    private List<String> dataflowServiceOptions;
    private List<String> experiments;

    public static void setOptions(
            final PipelineOptions pipelineOptions,
            final DataflowOptions dataflow) {

        if(dataflow == null) {
            return;
        }

        try {
            final Class<? extends PipelineOptions> clazz = (Class<? extends PipelineOptions>)Class.forName("org.apache.beam.runners.dataflow.options.DataflowPipelineOptions");
            final PipelineOptions dataflowOptions = pipelineOptions.as(clazz);

            if(dataflow.tempLocation != null) {
                pipelineOptions.setTempLocation(dataflow.tempLocation);
            }
            if(dataflow.stagingLocation != null) {
                clazz.getMethod("setStagingLocation", String.class).invoke(dataflowOptions, dataflow.stagingLocation);
            }
            if(dataflow.labels != null && !dataflow.labels.isEmpty()) {
                clazz.getMethod("setLabels", Map.class).invoke(dataflowOptions, dataflow.labels);
            }
            if(dataflow.autoscalingAlgorithm != null) {
                final Class enumClazz = Class.forName("org.apache.beam.runners.dataflow.options.DataflowPipelineWorkerPoolOptions.AutoscalingAlgorithmType");
                clazz.getMethod("setAutoscalingAlgorithm", Enum.class).invoke(dataflowOptions, Enum.valueOf(enumClazz, dataflow.autoscalingAlgorithm));
            }
            if(dataflow.flexRSGoal != null) {
                final Class enumClazz = Class.forName("org.apache.beam.runners.dataflow.options.DataflowPipelineOptions.FlexResourceSchedulingGoal");
                clazz.getMethod("setFlexRSGoal", Enum.class).invoke(dataflowOptions, Enum.valueOf(enumClazz, dataflow.flexRSGoal));
            }
            if(dataflow.numWorkers != null && dataflow.numWorkers > 0) {
                clazz.getMethod("setNumWorkers", int.class).invoke(dataflowOptions, dataflow.numWorkers);
            }
            if(dataflow.maxNumWorkers != null && dataflow.maxNumWorkers > 0) {
                clazz.getMethod("setMaxNumWorkers", int.class).invoke(dataflowOptions, dataflow.maxNumWorkers);
            }
            if(dataflow.numberOfWorkerHarnessThreads != null && dataflow.numberOfWorkerHarnessThreads > 0) {
                clazz.getMethod("setNumberOfWorkerHarnessThreads", int.class).invoke(dataflowOptions, dataflow.numberOfWorkerHarnessThreads);
            }
            if(dataflow.workerMachineType != null) {
                clazz.getMethod("setWorkerMachineType", String.class).invoke(dataflowOptions, dataflow.workerMachineType);
            }
            if(dataflow.workerRegion != null) {
                pipelineOptions.as(GcpOptions.class).setWorkerRegion(dataflow.workerRegion);
            }
            if(dataflow.workerZone != null) {
                pipelineOptions.as(GcpOptions.class).setWorkerZone(dataflow.workerZone);
            }
            if(dataflow.diskSizeGb != null) {
                clazz.getMethod("setDiskSizeGb", int.class).invoke(dataflowOptions, dataflow.diskSizeGb);
            }
            if(dataflow.workerDiskType != null) {
                clazz.getMethod("setWorkerDiskType", String.class).invoke(dataflowOptions, dataflow.workerDiskType);
            }
            if(dataflow.serviceAccount != null) {
                clazz.getMethod("setServiceAccount", String.class).invoke(dataflowOptions, dataflow.serviceAccount);
            }
            if(dataflow.impersonateServiceAccount != null) {
                pipelineOptions.as(GcpOptions.class).setImpersonateServiceAccount(dataflow.impersonateServiceAccount);
            }
            if(dataflow.network != null) {
                clazz.getMethod("setNetwork", String.class).invoke(dataflowOptions, dataflow.network);
            }
            if(dataflow.subnetwork != null) {
                clazz.getMethod("setSubnetwork", String.class).invoke(dataflowOptions, dataflow.subnetwork);
            }
            if(dataflow.usePublicIps != null) {
                clazz.getMethod("setUsePublicIps", Boolean.class).invoke(dataflowOptions, dataflow.usePublicIps);
            }
            if(dataflow.workerCacheMb != null && dataflow.workerCacheMb > 0) {
                clazz.getMethod("setWorkerCacheMb", Integer.class).invoke(dataflowOptions, dataflow.workerCacheMb);
            }
            if(dataflow.createFromSnapshot != null) {
                clazz.getMethod("setCreateFromSnapshot", String.class).invoke(dataflowOptions, dataflow.createFromSnapshot);
            }
            if(dataflow.sdkContainerImage != null) {
                clazz.getMethod("setSdkContainerImage", String.class).invoke(dataflowOptions, dataflow.sdkContainerImage);
            }
            if(dataflow.streamingSideInputCacheExpirationMillis != null) {
                clazz.getMethod("setStreamingSideInputCacheExpirationMillis", Integer.class).invoke(dataflowOptions, dataflow.streamingSideInputCacheExpirationMillis);
            }
            if(dataflow.update != null) {
                clazz.getMethod("setUpdate", Boolean.class).invoke(dataflowOptions, dataflow.update);
                if(dataflow.update) {
                    if(dataflow.transformNameMapping != null && !dataflow.transformNameMapping.isEmpty()) {
                        clazz.getMethod("setTransformNameMapping", Map.class).invoke(dataflowOptions, dataflow.transformNameMapping);
                    }
                }
            }
            if(dataflow.enableStreamingEngine != null && pipelineOptions.as(StreamingOptions.class).isStreaming()) {
                dataflowOptions.as(GcpOptions.class).setEnableStreamingEngine(dataflow.enableStreamingEngine);
            }
            if(dataflow.dataflowServiceOptions != null && !dataflow.dataflowServiceOptions.isEmpty()) {
                final List<String> existingDataflowServiceOptions = Optional
                        .ofNullable((List<String>)clazz.getMethod("getDataflowServiceOptions").invoke(dataflowOptions))
                        .orElseGet(ArrayList::new);
                existingDataflowServiceOptions.addAll(dataflow.dataflowServiceOptions);
                clazz.getMethod("setDataflowServiceOptions", List.class).invoke(dataflowOptions, existingDataflowServiceOptions.stream().distinct().toList());
            }
            if(dataflow.experiments != null && !dataflow.experiments.isEmpty()) {
                final List<String> existingExperiments = Optional
                        .ofNullable((List<String>)clazz.getMethod("getExperiments").invoke(dataflowOptions))
                        .orElseGet(ArrayList::new);
                existingExperiments.addAll(dataflow.experiments);
                clazz.getMethod("setExperiments", List.class).invoke(dataflowOptions, existingExperiments.stream().distinct().toList());
            }
        } catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
            throw new RuntimeException("Failed to set dataflow runner pipeline options", e);
        }

    }

    public static DataflowOptions copy(final PipelineOptions pipelineOptions) {
        final DataflowOptions dataflow = new DataflowOptions();
        try {
            final Class<? extends PipelineOptions> clazz = (Class<? extends PipelineOptions>)Class.forName("org.apache.beam.runners.dataflow.options.DataflowPipelineOptions");
            final PipelineOptions dataflowOptions = pipelineOptions.as(clazz);

            if(clazz.getMethod("getTempLocation").invoke(dataflowOptions) != null) {
                dataflow.tempLocation = (String)clazz.getMethod("getTempLocation").invoke(dataflowOptions);
            }
            if(clazz.getMethod("getStagingLocation").invoke(dataflowOptions) != null) {
                dataflow.stagingLocation = (String)clazz.getMethod("getStagingLocation").invoke(dataflowOptions);
            }
            if(clazz.getMethod("getLabels").invoke(dataflowOptions) != null) {
                dataflow.labels = (Map<String, String>)clazz.getMethod("getLabels").invoke(dataflowOptions);
            }
            /*
            if(dataflow.autoscalingAlgorithm != null) {
                final Class enumClazz = Class.forName("org.apache.beam.runners.dataflow.options.DataflowPipelineWorkerPoolOptions.AutoscalingAlgorithmType");
                clazz.getMethod("setAutoscalingAlgorithm", Enum.class).invoke(dataflowOptions, Enum.valueOf(enumClazz, dataflow.autoscalingAlgorithm));
            }
            if(dataflow.flexRSGoal != null) {
                final Class enumClazz = Class.forName("org.apache.beam.runners.dataflow.options.DataflowPipelineOptions.FlexResourceSchedulingGoal");
                clazz.getMethod("setFlexRSGoal", Enum.class).invoke(dataflowOptions, Enum.valueOf(enumClazz, dataflow.flexRSGoal));
            }
             */
            if(clazz.getMethod("getNumWorkers").invoke(dataflowOptions) != null) {
                dataflow.numWorkers = (Integer) clazz.getMethod("getNumWorkers").invoke(dataflowOptions);
            }
            if(clazz.getMethod("getMaxNumWorkers").invoke(dataflowOptions) != null) {
                dataflow.maxNumWorkers = (Integer) clazz.getMethod("getMaxNumWorkers").invoke(dataflowOptions);
            }
            if(clazz.getMethod("getNumberOfWorkerHarnessThreads").invoke(dataflowOptions) != null) {
                dataflow.numberOfWorkerHarnessThreads = (Integer) clazz.getMethod("getNumberOfWorkerHarnessThreads").invoke(dataflowOptions);
            }
            if(clazz.getMethod("getWorkerMachineType").invoke(dataflowOptions) != null) {
                dataflow.workerMachineType = (String)clazz.getMethod("getWorkerMachineType").invoke(dataflowOptions);
            }
            if(clazz.getMethod("getWorkerRegion").invoke(dataflowOptions) != null) {
                dataflow.workerRegion = (String)clazz.getMethod("getWorkerRegion").invoke(dataflowOptions);
            }
            if(clazz.getMethod("getWorkerZone").invoke(dataflowOptions) != null) {
                dataflow.workerZone = (String)clazz.getMethod("getWorkerZone").invoke(dataflowOptions);
            }
            if(clazz.getMethod("getDiskSizeGb").invoke(dataflowOptions) != null) {
                dataflow.diskSizeGb = (Integer) clazz.getMethod("getDiskSizeGb").invoke(dataflowOptions);
            }
            if(clazz.getMethod("getWorkerDiskType").invoke(dataflowOptions) != null) {
                dataflow.workerDiskType = (String)clazz.getMethod("getWorkerDiskType").invoke(dataflowOptions);
            }
            if(clazz.getMethod("getServiceAccount").invoke(dataflowOptions) != null) {
                dataflow.serviceAccount = (String)clazz.getMethod("getServiceAccount").invoke(dataflowOptions);
            }
            if(clazz.getMethod("getImpersonateServiceAccount").invoke(dataflowOptions) != null) {
                dataflow.impersonateServiceAccount = (String)clazz.getMethod("getImpersonateServiceAccount").invoke(dataflowOptions);
            }
            if(clazz.getMethod("getNetwork").invoke(dataflowOptions) != null) {
                dataflow.network = (String)clazz.getMethod("getNetwork").invoke(dataflowOptions);
            }
            if(clazz.getMethod("getSubnetwork").invoke(dataflowOptions) != null) {
                dataflow.subnetwork = (String)clazz.getMethod("getSubnetwork").invoke(dataflowOptions);
            }
            if(clazz.getMethod("getUsePublicIps").invoke(dataflowOptions) != null) {
                dataflow.usePublicIps = (Boolean) clazz.getMethod("getUsePublicIps").invoke(dataflowOptions);
            }
            if(clazz.getMethod("getWorkerCacheMb").invoke(dataflowOptions) != null) {
                dataflow.workerCacheMb = (Integer) clazz.getMethod("getWorkerCacheMb").invoke(dataflowOptions);
            }
            if(clazz.getMethod("getCreateFromSnapshot").invoke(dataflowOptions) != null) {
                dataflow.createFromSnapshot = (String)clazz.getMethod("getCreateFromSnapshot").invoke(dataflowOptions);
            }
            if(clazz.getMethod("getSdkContainerImage").invoke(dataflowOptions) != null) {
                dataflow.sdkContainerImage = (String)clazz.getMethod("getSdkContainerImage").invoke(dataflowOptions);
            }
            if(clazz.getMethod("getStreamingSideInputCacheExpirationMillis").invoke(dataflowOptions) != null) {
                dataflow.streamingSideInputCacheExpirationMillis = (Integer) clazz.getMethod("getStreamingSideInputCacheExpirationMillis").invoke(dataflowOptions);
            }
            if(pipelineOptions.as(StreamingOptions.class).isStreaming()) {
                if(clazz.getMethod("getUpdate").invoke(dataflowOptions) != null) {
                    dataflow.update = (Boolean) clazz.getMethod("getUpdate").invoke(dataflowOptions);
                    if(dataflow.update) {
                        if(clazz.getMethod("getTransformNameMapping").invoke(dataflowOptions) != null) {
                            dataflow.transformNameMapping = (Map<String, String>) clazz.getMethod("getTransformNameMapping").invoke(dataflowOptions);
                        }
                    }
                }
                if(clazz.getMethod("getEnableStreamingEngine").invoke(dataflowOptions) != null) {
                    dataflow.enableStreamingEngine = (Boolean) clazz.getMethod("getEnableStreamingEngine").invoke(dataflowOptions);
                }
            }

            if(clazz.getMethod("getDataflowServiceOptions").invoke(dataflowOptions) != null) {
                dataflow.dataflowServiceOptions = (List<String>) clazz.getMethod("getDataflowServiceOptions").invoke(dataflowOptions);
            }
            if(clazz.getMethod("getExperiments").invoke(dataflowOptions) != null) {
                dataflow.experiments = (List<String>) clazz.getMethod("getExperiments").invoke(dataflowOptions);
            }
        } catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
            throw new RuntimeException("Failed to set dataflow runner pipeline options", e);
        }
        return dataflow;
    }

    public static Integer getMaxNumWorkers(final PInput input) {
        try {
            final Class<? extends PipelineOptions> clazz = (Class<? extends PipelineOptions>)Class.forName("org.apache.beam.runners.dataflow.options.DataflowPipelineOptions");
            final PipelineOptions dataflowOptions = input.getPipeline().getOptions().as(clazz);
            return (Integer)clazz.getMethod("getMaxNumWorkers").invoke(dataflowOptions);
        } catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
            throw new RuntimeException("Failed to set dataflow runner pipeline options", e);
        }
    }

    public static String getProject(final PipelineOptions options) {
        return options.as(GcpOptions.class).getProject();
    }

    public static String getServiceAccount(final PipelineOptions options) {
        try {
            final Class<? extends PipelineOptions> clazz = (Class<? extends PipelineOptions>)Class.forName("org.apache.beam.runners.dataflow.options.DataflowPipelineOptions");
            final PipelineOptions dataflowOptions = options.as(clazz);
            return (String)clazz.getMethod("getServiceAccount").invoke(dataflowOptions);
        } catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
            throw new RuntimeException("Failed to getServiceAccount", e);
        }
    }

}
