package com.mercari.solution.module;

import com.google.common.reflect.ClassPath;
import com.google.gson.Gson;
import com.mercari.solution.MPipeline;
import com.mercari.solution.config.SourceConfig;
import com.mercari.solution.util.pipeline.OptionUtil;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.io.IOException;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public abstract class Source extends Module<PBegin> {

    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.TYPE)
    public @interface Module {
        String name();
    }

    public enum Mode {
        batch,
        streaming,
        microBatch,
        changeDataCapture,
        view
    }

    private static final Map<String, Class<Source>> sources = findSourcesInPackage("com.mercari.solution.module.source");

    private Schema schema;

    private String timestampAttribute;
    private String timestampDefault;

    private Mode mode;

    public Schema getSchema() {
        return schema;
    }

    public String getTimestampAttribute() {
        return timestampAttribute;
    }

    public String getTimestampDefault() {
        return timestampDefault;
    }

    public Mode getMode() {
        return mode;
    }

    private void setup(
            final @NonNull SourceConfig config,
            final Module properties,
            final PipelineOptions options,
            final List<MCollection> waits) {

        super.setup(config, options, waits);
        this.schema = config.getSchema();
        this.timestampAttribute = config.getTimestampAttribute();
        this.timestampDefault = config.getTimestampDefault();

        this.mode = Optional
                .ofNullable(config.getMode())
                .orElseGet(() -> OptionUtil.isStreaming(options) ? Mode.streaming : Mode.batch);
        if(this.schema != null) {
            schema.setup();
        }
    }

    public static @NonNull Source create(
            @NonNull SourceConfig config,
            @NonNull PipelineOptions options,
            List<MCollection> waits) {

        return Optional.ofNullable(sources.get(config.getModule())).map(clazz -> {
            final Source module;
            try {
                module = clazz.getDeclaredConstructor().newInstance();
            } catch (InstantiationException
                     | IllegalAccessException
                     | NoSuchMethodException
                     | InvocationTargetException e) {

                final String errorMessage = "Failed to instantiate source module: " + config.getModule() + ", class: " + clazz;
                throw new IllegalModuleException(errorMessage, e);
            }
            final Module properties = module.getClass().getAnnotation(Module.class);
            module.setup(config, properties, options, waits);
            return module;
        }).orElseThrow(() -> new IllegalModuleException("Not supported source module: " + config.getModule()));
    }

    private static Map<String, Class<Source>> findSourcesInPackage(String packageName) {
        final ClassPath classPath;
        try {
            ClassLoader loader = Source.class.getClassLoader();
            classPath = ClassPath.from(loader);
        } catch (IOException ioe) {
            throw new RuntimeException("Reading classpath resource failed", ioe);
        }
        return classPath.getTopLevelClassesRecursive(packageName)
                .stream()
                .map(ClassPath.ClassInfo::load)
                .filter(Source.class::isAssignableFrom)
                .map(clazz -> (Class<Source>)clazz.asSubclass(Source.class))
                .peek(clazz -> {
                    if(!clazz.isAnnotationPresent(Module.class)) {
                        throw new IllegalArgumentException("source module: " + clazz.getName() + " must have Module.Source annotation");
                    }
                })
                .collect(Collectors.toMap(
                        c -> c.getAnnotation(Module.class).name(),
                        c -> c));
    }

}