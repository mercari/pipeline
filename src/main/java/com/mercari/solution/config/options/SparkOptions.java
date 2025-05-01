package com.mercari.solution.config.options;

import org.apache.beam.sdk.options.PipelineOptions;

import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.util.List;

public class SparkOptions implements Serializable {

    // SparkCommonPipelineOptions
    private String sparkMaster;
    private String checkpointDir;
    private String storageLevel;
    private Boolean enableSparkMetricSinks;
    private Boolean preferGroupByKeyToHandleHugeValues;

    // SparkPipelineOptions
    private Long bundleSize;
    private Long batchIntervalMillis;
    private Long checkpointDurationMillis;
    private Long maxRecordsPerBatch;
    private Long minReadTimeMillis;
    private Double readTimePercentage;
    private Boolean cacheDisabled;
    private Boolean usesProvidedSparkContext;
    private List<String> filesToStage;

    public static void setOptions(
            final PipelineOptions pipelineOptions,
            final SparkOptions spark) {

        if(spark == null) {
            return;
        }

        try {
            final Class<? extends PipelineOptions> clazz = (Class<? extends PipelineOptions>) Class.forName("org.apache.beam.runners.spark.SparkPipelineOptions");
            final PipelineOptions sparkOptions = pipelineOptions.as(clazz);

            if(spark.sparkMaster != null) {
                clazz.getMethod("setSparkMaster", String.class).invoke(sparkOptions, spark.sparkMaster);
            }
            if(spark.checkpointDir != null) {
                clazz.getMethod("setCheckpointDir", String.class).invoke(sparkOptions, spark.checkpointDir);
            }
            if(spark.storageLevel != null) {
                clazz.getMethod("setStorageLevel", String.class).invoke(sparkOptions, spark.storageLevel);
            }
            if(spark.enableSparkMetricSinks != null) {
                clazz.getMethod("setEnableSparkMetricSinks", Boolean.class).invoke(sparkOptions, spark.enableSparkMetricSinks);
            }
            if(spark.preferGroupByKeyToHandleHugeValues != null) {
                clazz.getMethod("setPreferGroupByKeyToHandleHugeValues", Boolean.class).invoke(sparkOptions, spark.preferGroupByKeyToHandleHugeValues);
            }

            if(spark.bundleSize != null) {
                clazz.getMethod("setBundleSize", Long.class).invoke(sparkOptions, spark.bundleSize);
            }
            if(spark.batchIntervalMillis != null) {
                clazz.getMethod("setBatchIntervalMillis", Long.class).invoke(sparkOptions, spark.batchIntervalMillis);
            }
            if(spark.checkpointDurationMillis != null) {
                clazz.getMethod("setCheckpointDurationMillis", Long.class).invoke(sparkOptions, spark.checkpointDurationMillis);
            }
            if(spark.maxRecordsPerBatch != null) {
                clazz.getMethod("setMaxRecordsPerBatch", Long.class).invoke(sparkOptions, spark.maxRecordsPerBatch);
            }
            if(spark.minReadTimeMillis != null) {
                clazz.getMethod("setMinReadTimeMillis", Long.class).invoke(sparkOptions, spark.minReadTimeMillis);
            }
            if(spark.cacheDisabled != null) {
                clazz.getMethod("setCacheDisabled", Boolean.class).invoke(sparkOptions, spark.cacheDisabled);
            }
            if(spark.usesProvidedSparkContext != null) {
                clazz.getMethod("setUsesProvidedSparkContext", Boolean.class).invoke(sparkOptions, spark.usesProvidedSparkContext);
            }

        } catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
            throw new RuntimeException("Failed to set spark runner pipeline options", e);
        }
    }

}
