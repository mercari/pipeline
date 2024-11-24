package com.mercari.solution.module;

import com.google.common.reflect.ClassPath;
import com.mercari.solution.config.SinkConfig;
import com.mercari.solution.util.pipeline.Union;
import org.apache.beam.sdk.options.PipelineOptions;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.io.IOException;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public abstract class Sink extends Module<MCollectionTuple> {

    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.TYPE)
    public @interface Module {
        String name();
    }

    private static final Map<String, Class<Sink>> sinks = findSinksInPackage("com.mercari.solution.module.sink");

    private Schema schema;
    private Strategy strategy;
    private Union.Parameters union;
    private List<String> inputNames;

    public final Schema getSchema() {
        return schema;
    }

    public final Strategy getStrategy() {
        return strategy;
    }

    public final Union.Parameters getUnion() {
        return union;
    }

    public List<String> getInputNames() {
        return inputNames;
    }

    private void setup(
            final @NonNull SinkConfig config,
            final Module properties,
            final PipelineOptions options,
            final List<MCollection> waits) {

        super.setup(config, options, waits);
        this.schema = config.getSchema();
        this.strategy = Optional
                .ofNullable(config.getStrategy())
                .orElseGet(Strategy::createDefaultStrategy);
        this.union = Optional
                .ofNullable(config.getUnion())
                .orElseGet(Union.Parameters::createDefaultParameter);
        this.inputNames = config.getInputs();
    }

    public static @NonNull Sink create(
            final @NonNull SinkConfig config,
            final @NonNull PipelineOptions options,
            final @NonNull List<MCollection> waits) {

        return Optional.ofNullable(sinks.get(config.getModule())).map(clazz -> {
            final Sink module;
            try {
                module = clazz.getDeclaredConstructor().newInstance();
            } catch (InstantiationException | IllegalAccessException | NoSuchMethodException | InvocationTargetException e) {
                throw new RuntimeException("Failed to instantiate sinks module: " + config.getModule() + ", class: " + clazz, e);
            }
            final Module properties = module.getClass().getAnnotation(Module.class);
            module.setup(config, properties, options, waits);
            return module;
        }).orElseThrow(() -> new IllegalArgumentException("Not supported sinks module: " + config.getModule()));
    }

    private static Map<String, Class<Sink>> findSinksInPackage(String packageName) {
        final ClassPath classPath;
        try {
            ClassLoader loader = Sink.class.getClassLoader();
            classPath = ClassPath.from(loader);
        } catch (IOException ioe) {
            throw new RuntimeException("Reading classpath resource failed", ioe);
        }
        return classPath.getTopLevelClassesRecursive(packageName)
                .stream()
                .map(ClassPath.ClassInfo::load)
                .filter(Sink.class::isAssignableFrom)
                .map(clazz -> (Class<Sink>)clazz.asSubclass(Sink.class))
                .peek(clazz -> {
                    if(!clazz.isAnnotationPresent(Module.class)) {
                        throw new IllegalArgumentException("sink module: " + clazz.getName() + " must have Module.Sink annotation");
                    }
                })
                .collect(Collectors.toMap(
                        c -> c.getAnnotation(Module.class).name(),
                        c -> c));
    }

}
