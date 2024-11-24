package com.mercari.solution;

import com.mercari.solution.config.*;
import com.mercari.solution.module.*;
import com.mercari.solution.util.gcp.PubSubUtil;
import com.mercari.solution.util.gcp.StorageUtil;
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

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

public class MPipeline {

    private static final Logger LOG = LoggerFactory.getLogger(MPipeline.class);

    public interface MPipelineOptions extends PipelineOptions {

        @Description("Config json text body or gcs path.")
        String getConfig();
        void setConfig(String config);

        @Description("Config file type")
        @Default.String("json")
        String getType();
        void setType(String type);

        @Description("Use config template engine")
        @Default.Boolean(true)
        Boolean getUseConfigTemplate();
        void setUseConfigTemplate(Boolean useConfigTemplate);

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

        final Config config = loadConfig(pipelineOptions.getConfig(), args, pipelineOptions.getUseConfigTemplate());
        if(Optional.ofNullable(config.getEmpty()).orElse(false)) {
            LOG.info("Empty pipeline");
            final Pipeline pipeline = Pipeline.create(pipelineOptions);
            pipeline.apply("Empty", Create.of("").withCoder(StringUtf8Coder.of()));
            pipeline.run();
            return;
        }
        Options.setOptions(pipelineOptions, config.getOptions());

        final Pipeline pipeline = Pipeline.create(pipelineOptions);

        final Map<String, MCollection> outputs = apply(pipeline, config, args);

        for(final Map.Entry<String, MCollection> entry : outputs.entrySet()) {
            if(entry.getKey().endsWith(".failures")) {
                continue;
            }
            LOG.info("output: {}, schema: {}", entry.getKey(), entry.getValue().getSchema());
        }

        final PipelineResult result = pipeline.run();
    }

    public static Map<String, MCollection> apply(final Pipeline pipeline, final Config config) {
        return apply(pipeline, config, new String[]{});
    }

    public static Map<String, MCollection> apply(final Pipeline pipeline, final Config config, final String[] args) {

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

    public static Config loadConfig(final String configParam) throws Exception {
        return loadConfig(configParam, new String[0], true);
    }

    public static Config loadConfig(final String configParam, final String[] args, final Boolean useConfigTemplate) throws Exception {
        if(configParam == null) {
            throw new IllegalArgumentException("Parameter config must not be null!");
        }

        final String content;
        if(configParam.startsWith("gs://")) {
            LOG.info("config parameter is GCS path: {}", configParam);
            content = StorageUtil.readString(configParam);
        } else if(configParam.startsWith("data:")) {
            LOG.info("config parameter is base64 encoded");
            content = new String(Base64.getDecoder().decode(configParam), StandardCharsets.UTF_8);
        } else if(PubSubUtil.isSubscriptionResource(configParam)) {
            LOG.info("config parameter is PubSub Subscription: {}", configParam);
            content = PubSubUtil.getTextMessage(configParam);
            if(content == null) {
                final Config config = new Config();
                config.setEmpty(true);
                return config;
            }
            LOG.info("config content: {}", content);
        } else  {
            Path path;
            try {
                path = Paths.get(configParam);
            } catch (Throwable e) {
                path = null;
            }
            if(path != null && Files.exists(path) && !Files.isDirectory(path)) {
                LOG.info("config parameter is local file path: {}", configParam);
                content = Files.readString(path, StandardCharsets.UTF_8);
            } else {
                LOG.info("config parameter is json body: {}", configParam);
                content = configParam;
            }
        }

        if(content == null) {
            throw new IllegalArgumentException("Content is null for config parameter: " + configParam);
        }

        return Config.parse(content, args, useConfigTemplate);
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

            if(sourceConfig.getWait() != null && !outputs.keySet().containsAll(sourceConfig.getWait())) {
                notDoneModules.add(sourceConfig);
                continue;
            }

            final List<MCollection> waits;
            if(sourceConfig.getWait() == null) {
                waits = new ArrayList<>();
            } else {
                waits = sourceConfig.getWait().stream()
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
            if(transformConfig.getWait() != null && !outputs.keySet().containsAll(transformConfig.getWait())) {
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
            if(transformConfig.getWait() == null) {
                waits = new ArrayList<>();
            } else {
                waits = transformConfig.getWait().stream()
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
            if(sinkConfig.getWait() != null && !outputs.keySet().containsAll(sinkConfig.getWait())) {
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
            if(sinkConfig.getWait() == null) {
                waits = new ArrayList<>();
            } else {
                waits = sinkConfig.getWait().stream()
                        .map(outputs::get)
                        .collect(Collectors.toList());
            }

            final List<MCollection> inputs = sinkConfig.getInputs().stream()
                    .map(outputs::get)
                    .collect(Collectors.toList());

            try {
                final MCollectionTuple input = MCollectionTuple.mergeCollection(inputs);
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
