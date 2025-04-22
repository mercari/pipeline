package com.mercari.solution.module;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.mercari.solution.MPipeline;
import com.mercari.solution.config.Config;
import com.mercari.solution.config.ModuleConfig;
import com.mercari.solution.util.pipeline.OptionUtil;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.errorhandling.ErrorHandler;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PInput;

import java.util.*;

public abstract class Module<T extends PInput> extends PTransform<T, MCollectionTuple> {

    private String name;
    private String module;
    private String jobName;
    private String description;

    private String parametersText;

    private Set<String> tags;
    private transient List<PCollection<?>> waits;
    private List<Logging> loggings;

    private Map<String, String> templateArgs;
    private Boolean failFast;
    private List<FailureSink> failureSinks;
    private Boolean outputFailure;
    private DataType outputType;

    private MPipeline.Runner runner;


    @Override
    public String getName() {
        return name;
    }

    public String getModule() {
        return module;
    }

    public String getJobName() {
        return jobName;
    }

    public String getDescription() {
        return description;
    }

    public String getParametersText() {
        return parametersText;
    }

    public List<Logging> getLoggings() {
        return loggings;
    }

    public List<PCollection<?>> getWaits() {
        return waits;
    }

    public Map<String, String> getTemplateArgs() {
        return templateArgs;
    }

    public Boolean getFailFast() {
        return failFast;
    }

    public List<FailureSink> getFailureSinks() {
        return failureSinks;
    }

    public Boolean getOutputFailure() {
        return outputFailure;
    }

    public DataType getOutputType() {
        return outputType;
    }

    public MPipeline.Runner getRunner() {
        return runner;
    }

    public boolean hasFailures() {
        return failureSinks != null && !failureSinks.isEmpty();
    }

    protected void setup(
            final ModuleConfig config,
            final PipelineOptions options,
            final List<MCollection> waits) {

        this.name = config.getName();
        this.module = config.getModule();
        this.jobName = options.getJobName();
        this.description = config.getDescription();
        this.parametersText = config.getParameters().toString();

        this.tags = Optional.ofNullable(config.getTags()).orElseGet(HashSet::new);
        this.waits = new ArrayList<>();
        for(final MCollection wait : waits) {
            this.waits.add(wait.getCollection());
        }
        this.loggings = Optional
                .ofNullable(config.getLoggings())
                .map(l -> {
                    l.forEach(ll -> ll.setModuleName(name));
                    return l;
                })
                .orElseGet(ArrayList::new);

        this.failFast = Optional
                .ofNullable(config.getFailFast())
                .orElseGet(() -> !OptionUtil.isStreaming(options));
        this.failureSinks = Optional
                .ofNullable(config.getFailures())
                .map(l -> l.stream().map(ll -> FailureSink.create(ll, config, options)).toList())
                .orElseGet(ArrayList::new);
        this.outputFailure = Optional
                .ofNullable(config.getOutputFailure())
                .orElse(false);
        this.outputType = config.getOutputType();
        this.runner = OptionUtil.getRunner(options);
        this.templateArgs = config.getArgs();
    }

    protected <ParameterT> ParameterT getParameters(Class<ParameterT> clazz) {
        try {
            final JsonObject parametersJson = Config.convertConfigJson(parametersText, Config.Format.json);
            final ParameterT parameters = new Gson().fromJson(parametersJson, clazz);
            if (parameters == null) {
                throw new IllegalModuleException("parameters must not be empty");
            }
            return parameters;
        } catch (final IllegalModuleException e) {
            throw e;
        } catch (final Throwable e) {
            throw new IllegalModuleException("Illegal parameters for class: " + clazz, e);
        }
    }

    protected ErrorHandler.BadRecordErrorHandler<?> registerErrorHandler(final PInput input) {
        if(hasFailures()) {
            final Pipeline pipeline = input.getPipeline();
            final FailureSink.FailureSinks failureSinks = FailureSink.merge(getFailureSinks());
            return pipeline.registerBadRecordErrorHandler(failureSinks);
        } else {
            return null;
        }
    }

}
