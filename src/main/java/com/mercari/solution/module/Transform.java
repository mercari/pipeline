package com.mercari.solution.module;

import com.google.common.reflect.ClassPath;
import com.mercari.solution.config.TransformConfig;
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


public abstract class Transform extends Module<MCollectionTuple> {

    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.TYPE)
    public @interface Module {
        String name();
    }

    private static final Map<String, Class<Transform>> transforms = findTransformsInPackage("com.mercari.solution.module.transform");

    private Strategy strategy;
    private List<String> inputs;
    private transient List<MCollection> sideInputs;

    public Strategy getStrategy() {
        return strategy;
    }

    public List<String> getInputs() {
        return inputs;
    }

    public List<MCollection> getSideInputs() {
        return sideInputs;
    }

    private void setup(
            final @NonNull TransformConfig config,
            final Module properties,
            final @NonNull PipelineOptions options,
            final List<MCollection> waits,
            final List<MCollection> sideInputs,
            final MErrorHandler errorHandler) {

        super.setup(config, options, waits, errorHandler);
        this.strategy = Optional
                .ofNullable(config.getStrategy())
                .orElseGet(Strategy::createDefaultStrategy);
        this.inputs = config.getInputs();
        this.sideInputs = sideInputs;
    }

    public static @NonNull Transform create(
            final @NonNull TransformConfig config,
            final @NonNull PipelineOptions options,
            final @NonNull List<MCollection> waits,
            final @NonNull List<MCollection> sideInputs,
            final @NonNull MErrorHandler errorHandler) {

        return Optional.ofNullable(transforms.get(config.getModule())).map(clazz -> {
            final Transform module;
            try {
                module = clazz.getDeclaredConstructor().newInstance();
            } catch (InstantiationException | IllegalAccessException | NoSuchMethodException | InvocationTargetException e) {

                throw new RuntimeException("Failed to instantiate transform module: " + config.getModule() + ", class: " + clazz, e);
            }
            final Module properties = module.getClass().getAnnotation(Module.class);
            module.setup(config, properties, options, waits, sideInputs, errorHandler);
            return module;
        }).orElseThrow(() -> new IllegalArgumentException("Not supported transform module: " + config.getModule()));
    }

    private static Map<String, Class<Transform>> findTransformsInPackage(String packageName) {
        final ClassPath classPath;
        try {
            ClassLoader loader = Transform.class.getClassLoader();
            classPath = ClassPath.from(loader);
        } catch (IOException ioe) {
            throw new RuntimeException("Reading classpath resource failed", ioe);
        }
        return classPath.getTopLevelClassesRecursive(packageName)
                .stream()
                .map(ClassPath.ClassInfo::load)
                .filter(Transform.class::isAssignableFrom)
                .map(clazz -> (Class<Transform>)clazz.asSubclass(Transform.class))
                .peek(clazz -> {
                    if(!clazz.isAnnotationPresent(Module.class)) {
                        throw new IllegalArgumentException("transform module: " + clazz.getName() + " must have Transform.Module annotation");
                    }
                })
                .collect(Collectors.toMap(
                        c -> c.getAnnotation(Module.class).name(),
                        c -> c));
    }

}
