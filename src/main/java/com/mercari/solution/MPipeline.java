package com.mercari.solution;

import com.mercari.solution.config.*;
import com.mercari.solution.module.*;
import com.mercari.solution.util.pipeline.OptionUtil;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

public class MPipeline {

    private static final Logger LOG = LoggerFactory.getLogger(MPipeline.class);

    public interface MPipelineOptions extends PipelineOptions {

        @Description("Config text body or config resource name.")
        String getConfig();
        void setConfig(String config);

        @Description("Context for pipeline job.")
        String getContext();
        void setContext(String context);

        @Description("Config format. json or yaml.")
        @Default.Enum("unknown")
        Config.Format getFormat();
        void setFormat(Config.Format format);

    }

    public enum Runner {
        direct,
        dataflow,
        prism,
        portable,
        flink,
        spark
    }

    public enum Platform {
        gcp,
        aws,
        azure,
        other
    }

    public static void main(final String[] args) throws Exception {

        final MPipelineOptions pipelineOptions = PipelineOptionsFactory
                .fromArgs(OptionUtil.filterPipelineArgs(args))
                .as(MPipelineOptions.class);
        final Runner runner = OptionUtil.getRunner(pipelineOptions);
        LOG.info("Runner: {}", runner);

        final Config config = Config.load(
                pipelineOptions.getConfig(),
                pipelineOptions.getContext(),
                pipelineOptions.getFormat(),
                args);

        if(Optional.ofNullable(config.getEmpty()).orElse(false)) {
            LOG.info("Empty pipeline");
            final Pipeline pipeline = Pipeline.create(pipelineOptions);
            pipeline.apply("Empty", Create.of("").withCoder(StringUtf8Coder.of()));
            pipeline.run();
            return;
        }

        Options.setOptions(pipelineOptions, config.getOptions());

        final Pipeline pipeline = Pipeline.create(pipelineOptions);

        final Map<String, MCollection> outputs = apply(pipeline, config);

        for(final Map.Entry<String, MCollection> entry : outputs.entrySet()) {
            if(entry.getKey().endsWith(".failures")) {
                continue;
            }
            LOG.info("output: {}, schema: {}", entry.getKey(), entry.getValue().getSchema());
        }

        final PipelineResult result = pipeline.run();
    }

    public static Map<String, MCollection> apply(final Pipeline pipeline, final Config config) {

        final Map<String, MCollection> outputs = new HashMap<>();
        final Set<String> executedModuleNames = new HashSet<>();
        final Set<String> moduleNames = moduleNames(config);

        final int size = moduleNames.size();
        int preOutputSize = 0;
        while(preOutputSize < size) {
            setSourceResult(pipeline, config.getSources(), outputs, executedModuleNames);
            setTransformResult(pipeline, config.getTransforms(), outputs, executedModuleNames);
            setSinkResult(pipeline, config.getSinks(), outputs, executedModuleNames);
            if(preOutputSize == executedModuleNames.size()) {
                moduleNames.removeAll(executedModuleNames);
                final String message = String.format("No input for modules: %s", String.join(",", moduleNames));
                throw new IllegalModuleException("", "pipeline", List.of(message));
            }
            preOutputSize = executedModuleNames.size();
        }

        return outputs;
    }

    private static void setSourceResult(
            final Pipeline pipeline,
            final List<SourceConfig> sourceConfigs,
            final Map<String, MCollection> outputs,
            final Set<String> executedModuleNames) {

        final List<SourceConfig> notDoneModules = new ArrayList<>();
        for (final SourceConfig sourceConfig : sourceConfigs) {
            // Skip null config(ketu comma)
            if(sourceConfig == null) {
                continue;
            }

            // Ignore if parameter ignore is true
            if(sourceConfig.getIgnore() != null && sourceConfig.getIgnore()) {
                continue;
            }

            // Skip already done module.
            if(executedModuleNames.contains(sourceConfig.getName())) {
                continue;
            }

            if(sourceConfig.getWaits() != null && !outputs.keySet().containsAll(sourceConfig.getWaits())) {
                notDoneModules.add(sourceConfig);
                continue;
            }

            final List<MCollection> waits;
            if(sourceConfig.getWaits() == null) {
                waits = new ArrayList<>();
            } else {
                waits = sourceConfig.getWaits().stream()
                        .map(outputs::get)
                        .toList();
            }

            try {
                final Source source = Source.create(sourceConfig, pipeline.getOptions(), waits);
                final MCollectionTuple output = pipeline.begin()
                        .apply(sourceConfig.getName(), source)
                        .withSource(sourceConfig.getName());
                outputs.putAll(output.asCollectionMap());
                executedModuleNames.add(sourceConfig.getName());
            } catch (final IllegalModuleException e) {
                throw new IllegalModuleException(sourceConfig.getName(), sourceConfig.getModule(), e.errorMessages);
            } catch (final Throwable e) {
                throw new IllegalModuleException(sourceConfig.getName(), sourceConfig.getModule(), e);
            }
        }

        if(notDoneModules.isEmpty()) {
            return;
        }
        if(notDoneModules.size() == sourceConfigs.size()) {
            return;
        }
        setSourceResult(pipeline, notDoneModules, outputs, executedModuleNames);
    }

    private static void setTransformResult(
            final Pipeline pipeline,
            final List<TransformConfig> transformConfigs,
            final Map<String, MCollection> outputs,
            final Set<String> executedModuleNames) {

        final List<TransformConfig> notDoneModules = new ArrayList<>();
        for(final TransformConfig transformConfig : transformConfigs) {
            // Skip null config(ketu comma)
            if(transformConfig == null) {
                continue;
            }

            // Ignore if parameter ignore is true
            if(transformConfig.getIgnore() != null && transformConfig.getIgnore()) {
                continue;
            }

            // Skip already done module.
            if(executedModuleNames.contains(transformConfig.getName())) {
                continue;
            }

            // Add queue if wait not done.
            if(transformConfig.getWaits() != null && !outputs.keySet().containsAll(transformConfig.getWaits())) {
                notDoneModules.add(transformConfig);
                continue;
            }

            // Add queue if sideInputs not done.
            if(transformConfig.getSideInputs() != null && !outputs.keySet().containsAll(transformConfig.getSideInputs())) {
                notDoneModules.add(transformConfig);
                continue;
            }

            // Add queue if all input not done.
            if(!outputs.keySet().containsAll(transformConfig.getInputs())) {
                notDoneModules.add(transformConfig);
                continue;
            }

            final List<MCollection> waits;
            if(transformConfig.getWaits() == null) {
                waits = new ArrayList<>();
            } else {
                waits = transformConfig.getWaits().stream()
                        .map(outputs::get)
                        .toList();
            }

            final List<MCollection> sideInputs;
            if(transformConfig.getSideInputs() == null) {
                sideInputs = new ArrayList<>();
            } else {
                sideInputs = transformConfig.getSideInputs().stream()
                        .map(outputs::get)
                        .toList();
            }

            final List<MCollection> inputs = transformConfig.getInputs().stream()
                    .map(outputs::get)
                    .collect(Collectors.toList());
            try {
                final MCollectionTuple input = MCollectionTuple.mergeCollection(inputs);
                final Transform transform = Transform.create(transformConfig, pipeline.getOptions(), waits, sideInputs);
                final MCollectionTuple output = input
                        .apply(transformConfig.getName(), transform)
                        .withSource(transformConfig.getName());
                outputs.putAll(output.asCollectionMap());
                executedModuleNames.add(transformConfig.getName());
            } catch (final IllegalModuleException e) {
                throw new IllegalModuleException(transformConfig.getName(), transformConfig.getModule(), e.errorMessages);
            } catch (final Throwable e) {
                throw new IllegalModuleException(transformConfig.getName(), transformConfig.getModule(), e);
            }
        }

        if(notDoneModules.isEmpty()) {
            return;
        }
        if(notDoneModules.size() == transformConfigs.size()) {
            return;
        }
        setTransformResult(pipeline, notDoneModules, outputs, executedModuleNames);
    }

    private static void setSinkResult(
            final Pipeline pipeline,
            final List<SinkConfig> sinkConfigs,
            final Map<String, MCollection> outputs,
            final Set<String> executedModuleNames) {

        final List<SinkConfig> notDoneModules = new ArrayList<>();
        for(final SinkConfig sinkConfig : sinkConfigs) {
            // Skip null config(ketu comma)
            if(sinkConfig == null) {
                continue;
            }

            // Ignore if parameter ignore is true
            if(sinkConfig.getIgnore() != null && sinkConfig.getIgnore()) {
                continue;
            }

            // Skip already done module.
            if(executedModuleNames.contains(sinkConfig.getName())) {
                continue;
            }

            // Add queue if wait not done.
            if(sinkConfig.getWaits() != null && !outputs.keySet().containsAll(sinkConfig.getWaits())) {
                notDoneModules.add(sinkConfig);
                continue;
            }

            // Add queue if input not done.
            if(!outputs.keySet().containsAll(sinkConfig.getInputs())) {
                notDoneModules.add(sinkConfig);
                continue;
            }

            // Add waits
            final List<MCollection> waits;
            if(sinkConfig.getWaits() == null) {
                waits = new ArrayList<>();
            } else {
                waits = sinkConfig.getWaits().stream()
                        .map(outputs::get)
                        .collect(Collectors.toList());
            }

            final List<MCollection> inputs = sinkConfig.getInputs().stream()
                    .map(outputs::get)
                    .collect(Collectors.toList());

            try {
                final MCollectionTuple input;
                if("action".equalsIgnoreCase(sinkConfig.getModule())) {
                    input = MCollectionTuple.empty(pipeline);
                } else {
                    input = MCollectionTuple.mergeCollection(inputs);
                }
                final Sink sink = Sink.create(sinkConfig, pipeline.getOptions(), waits);
                final MCollectionTuple output = input
                        .apply(sinkConfig.getName(), sink)
                        .withSource(sinkConfig.getName());
                outputs.putAll(output.asCollectionMap());
                executedModuleNames.add(sinkConfig.getName());
            } catch (final IllegalModuleException e) {
                throw new IllegalModuleException(sinkConfig.getName(), sinkConfig.getModule(), e.errorMessages);
            } catch (final Throwable e) {
                throw new IllegalModuleException(sinkConfig.getName(), sinkConfig.getModule(), e);
            }
        }

        if(notDoneModules.isEmpty()) {
            return;
        }
        if(notDoneModules.size() == sinkConfigs.size()) {
            return;
        }
        setSinkResult(pipeline, notDoneModules, outputs, executedModuleNames);
    }

    private static Set<String> moduleNames(final Config config) {
        final Set<String> moduleNames = new HashSet<>();
        moduleNames.addAll(config.getSources().stream()
                .filter(Objects::nonNull)
                .filter(c -> c.getIgnore() == null || !c.getIgnore())
                .map(SourceConfig::getName)
                .collect(Collectors.toSet()));
        moduleNames.addAll(config.getTransforms().stream()
                .filter(Objects::nonNull)
                .filter(c -> c.getIgnore() == null || !c.getIgnore())
                .map(TransformConfig::getName)
                .collect(Collectors.toSet()));
        moduleNames.addAll(config.getSinks().stream()
                .filter(Objects::nonNull)
                .filter(c -> c.getIgnore() == null || !c.getIgnore())
                .map(SinkConfig::getName)
                .collect(Collectors.toSet()));
        return moduleNames;
    }

}
