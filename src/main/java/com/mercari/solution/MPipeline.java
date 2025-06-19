package com.mercari.solution;

import com.mercari.solution.config.*;
import com.mercari.solution.module.*;
import com.mercari.solution.util.pipeline.OptionUtil;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.Create;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

public class MPipeline {

    private static final Logger LOG = LoggerFactory.getLogger(MPipeline.class);

    public interface MPipelineOptions extends PipelineOptions {

        @Description("Config text body or config resource name.")
        @Validation.Required
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

    public static void main(final String[] args) throws IOException {

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

        Options.setOptions(pipelineOptions, config.getOptions());

        final Pipeline pipeline = Pipeline.create(pipelineOptions);

        final Map<String, MCollection> outputs = apply(pipeline, pipelineOptions, args, config);

        for(final Map.Entry<String, MCollection> entry : outputs.entrySet()) {
            if(entry.getKey().endsWith(".failures")) {
                continue;
            }
            LOG.info("output: {}, schema: {}", entry.getKey(), entry.getValue().getSchema());
        }

        final PipelineResult result = pipeline.run();
    }

    private static Map<String, MCollection> apply(
            final Pipeline pipeline,
            final MPipelineOptions pipelineOptions,
            final String[] args,
            final Config config) throws IOException {

        if(Optional.ofNullable(config.getEmpty()).orElse(false)) {
            LOG.info("Empty pipeline");
            pipeline.apply("Empty", Create.of("").withCoder(StringUtf8Coder.of()));
            pipeline.run();
            return new HashMap<>();
        }

        try {
            return apply(pipeline, config);
        } catch (final Throwable e) {
            LOG.error("Failed to apply pipeline config: {}", config);
            if(config.getSystem().getFailure().getAlterConfig() == null) {
                throw e;
            }
            final Config alterConfig = Config.load(
                    config.getSystem().getFailure().getAlterConfig(),
                    pipelineOptions.getContext(),
                    pipelineOptions.getFormat(),
                    args);
            return apply(pipeline, pipelineOptions, args, alterConfig);
        }
    }

    public static Map<String, MCollection> apply(final Pipeline pipeline, final Config config) {

        final Map<String, MCollection> outputs = new HashMap<>();
        final Set<String> executedModuleNames = new HashSet<>();
        final Set<String> moduleNames = moduleNames(config);

        final int size = moduleNames.size();

        try(final MErrorHandler errorHandler = MErrorHandler.createPipelineErrorHandler(pipeline, config)) {
            int preOutputSize = 0;
            while(preOutputSize < size) {
                setResult(pipeline, config.getSources(), outputs, executedModuleNames, errorHandler);
                setResult(pipeline, config.getTransforms(), outputs, executedModuleNames, errorHandler);
                setResult(pipeline, config.getSinks(), outputs, executedModuleNames, errorHandler);
                if(preOutputSize == executedModuleNames.size()) {
                    moduleNames.removeAll(executedModuleNames);
                    final String message = String.format("No input for modules: %s", String.join(",", moduleNames));
                    throw new IllegalModuleException("", "pipeline", message);
                }
                preOutputSize = executedModuleNames.size();
            }
        }

        return outputs;
    }

    private static void setResult(
            final Pipeline pipeline,
            final List<? extends ModuleConfig> moduleConfigs,
            final Map<String, MCollection> outputs,
            final Set<String> executedModuleNames,
            final MErrorHandler errorHandler) {

        final List<ModuleConfig> notDoneModules = new ArrayList<>();
        for(final ModuleConfig moduleConfig : moduleConfigs) {
            // Skip null config(ketu comma)
            if(moduleConfig == null) {
                continue;
            }

            // Ignore if parameter ignore is true
            if(moduleConfig.getIgnore() != null && moduleConfig.getIgnore()) {
                continue;
            }

            // Skip already done module.
            if(executedModuleNames.contains(moduleConfig.getName())) {
                continue;
            }

            // Add queue if wait not done.
            if(!outputs.keySet().containsAll(moduleConfig.getWaits())) {
                notDoneModules.add(moduleConfig);
                continue;
            }
            final List<MCollection> waits = moduleConfig.getWaits()
                    .stream()
                    .map(outputs::get)
                    .toList();

            // Add queue if sideInputs not done.
            if(!outputs.keySet().containsAll(moduleConfig.getSideInputs())) {
                notDoneModules.add(moduleConfig);
                continue;
            }
            final List<MCollection> sideInputs = moduleConfig.getSideInputs()
                    .stream()
                    .map(outputs::get)
                    .toList();

            // Add queue if inputs not done.
            final List<String> inputNames = switch (moduleConfig) {
                case TransformConfig transformConfig -> transformConfig.getInputs();
                case SinkConfig sinkConfig -> sinkConfig.getInputs();
                default -> new ArrayList<>();
            };

            if(!outputs.keySet().containsAll(inputNames)) {
                notDoneModules.add(moduleConfig);
                continue;
            }
            final List<MCollection> inputs = inputNames.stream()
                    .map(outputs::get)
                    .collect(Collectors.toList());

            try {
                final MCollectionTuple output = switch (moduleConfig) {
                    case SourceConfig sourceConfig -> {
                        final Source source = Source.create(
                                sourceConfig, pipeline.getOptions(), waits, errorHandler);
                        yield pipeline
                                .begin()
                                .apply(moduleConfig.getName(), source);
                    }
                    case TransformConfig transformConfig -> {
                        final Transform transform = Transform.create(
                                transformConfig, pipeline.getOptions(), waits, sideInputs, errorHandler);
                        yield MCollectionTuple
                                .mergeCollection(inputs)
                                .apply(moduleConfig.getName(), transform);
                    }
                    case SinkConfig sinkConfig -> {
                        final Sink sink = Sink.create(
                                sinkConfig, pipeline.getOptions(), waits, errorHandler);
                        final MCollectionTuple input = inputs.isEmpty()
                                ? MCollectionTuple.empty(pipeline)
                                : MCollectionTuple.mergeCollection(inputs);
                        yield input.apply(moduleConfig.getName(), sink);
                    }
                    default -> throw new IllegalModuleException("Not supported config type: " + moduleConfig);
                };
                outputs.putAll(output.withSource(moduleConfig.getName()).asCollectionMap());
                executedModuleNames.add(moduleConfig.getName());

            } catch (final IllegalModuleException e) {
                throw new IllegalModuleException(moduleConfig.getName(), moduleConfig.getModule(), e.errorMessages);
            } catch (final Throwable e) {
                throw new IllegalModuleException(moduleConfig.getName(), moduleConfig.getModule(), e);
            }
        }

        if(notDoneModules.isEmpty()) {
            return;
        }
        if(notDoneModules.size() == moduleConfigs.size()) {
            return;
        }
        setResult(pipeline, notDoneModules, outputs, executedModuleNames, errorHandler);
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
