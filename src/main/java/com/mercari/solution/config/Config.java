package com.mercari.solution.config;

import com.google.common.io.CharStreams;
import com.google.gson.*;
import com.mercari.solution.module.IllegalModuleException;
import com.mercari.solution.util.TemplateUtil;
import com.mercari.solution.util.gcp.ArtifactRegistryUtil;
import com.mercari.solution.util.gcp.ParameterManagerUtil;
import com.mercari.solution.util.gcp.PubSubUtil;
import com.mercari.solution.util.gcp.StorageUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

import java.io.IOException;
import java.io.Reader;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;


public class Config implements Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(Config.class);

    // meta info
    private String name;
    private String version;
    private String description;
    private String content;
    private String resource;

    // system
    private Systems system;
    //// deprecated. use system.args
    private Map<String, String> args;
    //// deprecated. use system.imports
    private List<Import> imports;

    // pipeline config
    private Options options;
    private List<SourceConfig> sources;
    private List<TransformConfig> transforms;
    private List<SinkConfig> sinks;
    private List<FailureConfig> failures;

    private Boolean empty;

    // deprecated
    private Options settings;


    public String getName() {
        return name;
    }

    public String getDescription() {
        return description;
    }

    public Systems getSystem() {
        return system;
    }

    public List<Import> getImports() {
        return imports;
    }

    public Map<String, String> getArgs() {
        return args;
    }

    public Options getOptions() {
        return Optional
                .ofNullable(options)
                .orElse(settings);
    }

    public List<SourceConfig> getSources() {
        return sources;
    }

    public List<TransformConfig> getTransforms() {
        return transforms;
    }

    public List<SinkConfig> getSinks() {
        return sinks;
    }

    public List<FailureConfig> getFailures() {
        return failures;
    }

    public Boolean getEmpty() {
        return empty;
    }

    public void setEmpty(Boolean empty) {
        this.empty = empty;
    }

    public String getContent() {
        return content;
    }

    public void validate() {
        final List<String> errorMessages = new ArrayList<>();
        if(this.system != null) {
            errorMessages.addAll(this.system.validate());
        }
        if(this.imports != null) {
            for(int i=0; i<this.imports.size(); i++) {
                final Import imp = this.imports.get(i);
                imp.setDefaults(this.args);
                errorMessages.addAll(imp.validate(i));
            }
        }

        if(!errorMessages.isEmpty()) {
            throw new IllegalModuleException("", "pipeline", errorMessages);
        }
    }

    public void setDefaults(
            final String context,
            final Map<String, String> args) {

        if(this.system == null) {
            this.system = new Systems();
        }
        this.system.setDefaults(context, args);

        if(this.args == null) {
            this.args = new HashMap<>();
        }
        this.args.putAll(args);

        // deprecated
        if(this.imports == null) {
            this.imports = new ArrayList<>();
        } else {
            for(final Import i : this.imports) {
                i.setDefaults(this.args);
            }
        }
        if(!this.imports.isEmpty()) {
            this.system.imports = this.imports;
        }

        if(this.empty == null) {
            this.empty = false;
        }
    }

    public static class Systems implements Serializable {

        private Map<String, String> args;
        private String context;
        private List<Import> imports;
        private Failure failure;

        public Map<String, String> getArgs() {
            return args;
        }

        public String getContext() {
            return context;
        }

        public List<Import> getImports() {
            return imports;
        }

        public Failure getFailure() {
            return failure;
        }

        public List<String> validate() {
            final List<String> errorMessages = new ArrayList<>();
            return errorMessages;
        }

        public void setDefaults(
                final String context,
                final Map<String, String> args) {
            if(this.args == null) {
                this.args = new HashMap<>();
            }
            this.args.putAll(args);

            if(context != null && !context.isEmpty()) {
                this.context = context;
            }

            if(this.imports == null) {
                this.imports = new ArrayList<>();
            } else {
                for(final Import i : this.imports) {
                    i.setDefaults(this.args);
                }
            }

            if(this.failure == null) {
                this.failure = new Failure();
            }
            this.failure.setDefaults();
        }
    }

    public static class Failure implements Serializable {

        private Boolean failFast;
        private Boolean union;
        private String alterConfig;

        public Boolean getFailFast() {
            return failFast;
        }

        public Boolean getUnion() {
            return union;
        }

        public String getAlterConfig() {
            return alterConfig;
        }

        public List<String> validate() {
            final List<String> errorMessages = new ArrayList<>();
            return errorMessages;
        }

        public void setDefaults() {
            if(union == null) {
                union = false;
            }
        }
    }

    public static class Import implements Serializable {

        private String base;
        private List<String> files;
        private Map<String, String> args;

        public String getBase() {
            return base;
        }

        public List<String> getFiles() {
            return files;
        }

        public Map<String, String> getArgs() {
            return args;
        }

        public List<String> validate(int index) {
            final List<String> errorMessages = new ArrayList<>();
            if((this.files == null || this.files.isEmpty()) && this.base == null) {
                errorMessages.add("config.imports[" + index + "].files must not be empty" );
            }
            return errorMessages;
        }

        public void setDefaults(final Map<String, String> args) {
            if(this.args == null) {
                this.args = new HashMap<>();
            }
            if(args != null && !args.isEmpty()) {
                this.args.putAll(args);
            }
        }
    }

    public enum Format {
        json,
        yaml,
        unknown
    }

    public static Config empty() {
        final Config config = new Config();
        config.setEmpty(true);
        return config;
    }

    public static Config load(final String configParam) throws IOException {
        return load(configParam, null, Format.unknown, new String[0]);
    }

    public static Config load(final String configParam, final String context, final Format format, final String[] args) throws IOException {
        final Map<String, String> templateArgs = extractArgs(args);
        return load(configParam, context, format, templateArgs);
    }

    public static Config load(final String configParam, final String context, final Format format, final Map<String, String> args) throws IOException {
        if(configParam == null) {
            throw new IllegalModuleException("", "pipeline", List.of("pipeline parameter config must not be null"));
        }

        final String content;
        if(configParam.startsWith("gs://")) {
            LOG.info("config parameter is GCS path: {}", configParam);
            content = StorageUtil.readString(configParam);
        } else if(ParameterManagerUtil.isParameterVersionResource(configParam)) {
            LOG.info("config parameter is Parameter Manager resource: {}", configParam);
            final ParameterManagerUtil.Version version = ParameterManagerUtil.getParameterVersion(configParam);
            content = new String(version.payload, StandardCharsets.UTF_8);
        } else if(configParam.startsWith("ar://")) {
            LOG.info("config parameter is GAR path: {}", configParam);
            final byte[] bytes = ArtifactRegistryUtil.download(configParam);
            content = new String(bytes, StandardCharsets.UTF_8);
        } else if(configParam.startsWith("data:")) {
            LOG.info("config parameter is base64 encoded");
            content = new String(Base64.getDecoder().decode(configParam.replaceFirst("data:", "")), StandardCharsets.UTF_8);
        } else if(PubSubUtil.isSubscriptionResource(configParam)) {
            LOG.info("config parameter is PubSub Subscription: {}", configParam);
            content = PubSubUtil.getTextMessage(configParam);
            if(content == null) {
                return empty();
            }
            LOG.info("config content: {}", content);
        } else  {
            Path path;
            try {
                path = Paths.get(configParam);
            } catch (final Throwable e) {
                path = null;
            }
            if(path != null && Files.exists(path) && !Files.isDirectory(path)) {
                LOG.info("config parameter is local file path: {}", configParam);
                content = Files.readString(path, StandardCharsets.UTF_8);
            } else {
                LOG.info("config parameter body: {}", configParam);
                content = configParam;
            }
        }

        if(content == null) {
            throw new IllegalModuleException("", "pipeline", List.of("pipeline parameter config must not be empty"));
        }

        return parse(content, context, format, args);
    }

    public static Config parse(final String configText, final String context, final Format format, final String[] args) {
        final Map<String, String> templateArgs = extractArgs(args);
        return parse(configText, context, format, templateArgs);
    }

    public static Config parse(final String configText, final String context, final Format format, final Map<String, String> templateArgs) {
        try {
            JsonObject jsonObject = convertConfigJson(configText, format);
            LOG.info("Pipeline config: \n{}", new GsonBuilder().setPrettyPrinting().create().toJson(jsonObject));

            try {
                jsonObject = processArgs(jsonObject, templateArgs);
            } catch (Throwable e) {
                throw new IllegalModuleException("", "pipeline", e);
            }

            final Config config = new Gson().fromJson(jsonObject, Config.class);
            if(config == null) {
                throw new IllegalModuleException("", "pipeline", List.of("config must not be empty"));
            }
            config.validate();
            config.setDefaults(context, templateArgs);

            final List<FailureConfig> failures = Optional.ofNullable(config.getFailures()).orElseGet(ArrayList::new)
                    .stream()
                    .filter(Objects::nonNull)
                    .peek(c -> c.setArgs(templateArgs))
                    .peek(c -> c.applyContext(config.system.context))
                    .collect(Collectors.toList());

            final List<SourceConfig> sources = Optional.ofNullable(config.getSources()).orElseGet(ArrayList::new)
                    .stream()
                    .filter(Objects::nonNull)
                    .peek(c -> c.setArgs(templateArgs))
                    .peek(c -> c.applyContext(config.system.context))
                    .peek(c -> c.setFailFast(config.system.failure.failFast))
                    .peek(c -> c.addFailures(failures))
                    .collect(Collectors.toList());
            final List<TransformConfig> transforms = Optional.ofNullable(config.getTransforms()).orElseGet(ArrayList::new)
                    .stream()
                    .filter(Objects::nonNull)
                    .peek(c -> c.setArgs(templateArgs))
                    .peek(c -> c.applyContext(config.system.context))
                    .peek(c -> c.setFailFast(config.system.failure.failFast))
                    .peek(c -> c.addFailures(failures))
                    .collect(Collectors.toList());
            final List<SinkConfig> sinks = Optional.ofNullable(config.getSinks()).orElseGet(ArrayList::new)
                    .stream()
                    .filter(Objects::nonNull)
                    .peek(c -> c.setArgs(templateArgs))
                    .peek(c -> c.applyContext(config.system.context))
                    .peek(c -> c.setFailFast(config.system.failure.failFast))
                    .peek(c -> c.addFailures(failures))
                    .collect(Collectors.toList());

            for(final Import i : config.getSystem().getImports()) {
                for(final String path : i.getFiles()) {
                    final String configPath = (i.getBase() == null ? "" : i.getBase()) + path;
                    final Config importConfig = load(configPath, config.system.context, format, i.args);
                    if(importConfig.getSources() != null) {
                        sources.addAll(importConfig.getSources()
                                .stream()
                                .peek(c -> c.setArgs(i.args))
                                .peek(c -> c.applyContext(config.system.context))
                                .peek(c -> c.setFailFast(config.system.failure.failFast))
                                .peek(c -> c.addFailures(failures))
                                .toList());
                    }
                    if(importConfig.getTransforms() != null) {
                        transforms.addAll(importConfig.getTransforms()
                                .stream()
                                .peek(c -> c.setArgs(i.args))
                                .peek(c -> c.applyContext(config.system.context))
                                .peek(c -> c.setFailFast(config.system.failure.failFast))
                                .peek(c -> c.addFailures(failures))
                                .toList());
                    }
                    if(importConfig.getSinks() != null) {
                        sinks.addAll(importConfig.getSinks()
                                .stream()
                                .peek(c -> c.setArgs(i.args))
                                .peek(c -> c.applyContext(config.system.context))
                                .peek(c -> c.setFailFast(config.system.failure.failFast))
                                .peek(c -> c.addFailures(failures))
                                .toList());
                    }
                }
            }

            if(sources.isEmpty() && transforms.isEmpty() && sinks.isEmpty()) {
                throw new IllegalModuleException("", "pipeline", List.of("config has no module(sources,transforms,sinks) definition"));
            }

            config.sources = sources;
            config.transforms = transforms;
            config.sinks = sinks;
            config.failures = failures;

            config.content = jsonObject.toString();

            return config;
        } catch (Throwable e) {
            throw new IllegalModuleException("", "pipeline", e);
        }
    }

    public static JsonObject convertConfigJson(
            final Reader reader,
            final Format format) {
        try {
            final String configText = CharStreams.toString(reader);
            return convertConfigJson(configText, format);
        } catch (IOException e) {
            throw new IllegalModuleException("", "pipeline", e);
        }
    }

    public static JsonObject convertConfigJson(
            final String configText,
            final Format format) {

        switch (format) {
            case yaml -> {
                try {
                    return parseYaml(configText);
                } catch (final Throwable e) {
                    final String errorMessage = "Failed to parse config yaml error: " + e.getMessage() + ", yaml: " + configText;
                    LOG.error(errorMessage);
                    throw new IllegalModuleException("", "pipeline", List.of(errorMessage));
                }
            }
            case json -> {
                try {
                    return new Gson().fromJson(configText, JsonObject.class);
                } catch (final Throwable e) {
                    final String errorMessage = "Failed to parse config json error: " + e.getMessage() + ", json: " + configText;
                    LOG.error(errorMessage);
                    throw new IllegalModuleException("", "pipeline", List.of(errorMessage));
                }
            }
            default -> {
                try {
                    return new Gson().fromJson(configText, JsonObject.class);
                } catch (final JsonSyntaxException e) {
                    try {
                        return parseYaml(configText);
                    } catch (final Throwable ee) {
                        final String errorMessage = "Failed to parse config: " + configText;
                        LOG.error(errorMessage);
                        throw new IllegalModuleException(errorMessage, e);
                    }
                } catch (final Throwable e) {
                    final String errorMessage = "Failed to parse config json: " + configText;
                    LOG.error(errorMessage);
                    throw new IllegalModuleException(errorMessage, e);
                }
            }
        }
    }

    static Map<String, String> extractArgs(final String[] args) {
        final Map<String, Map<String, String>> argsParameters = filterConfigArgs(args);
        return new LinkedHashMap<>(argsParameters.getOrDefault("args", new LinkedHashMap<>()));
    }

    private static JsonObject processArgs(final JsonObject configJson, final Map<String, String> args) {
        final JsonObject argsJsonObject;
        if(configJson.has("args") && configJson.get("args").isJsonObject()) {
            argsJsonObject = configJson.getAsJsonObject("args");
        } else if(configJson.has("system") && configJson.get("system").isJsonObject()
                && configJson.getAsJsonObject("system").has("args")
                && configJson.getAsJsonObject("system").get("args").isJsonObject()) {

            argsJsonObject = configJson.getAsJsonObject("system").getAsJsonObject("args");
        } else {
            argsJsonObject = new JsonObject();
        }

        for(final Map.Entry<String, JsonElement> entry : argsJsonObject.entrySet()) {
            if(entry.getValue().isJsonPrimitive()) {
                final JsonPrimitive primitive = entry.getValue().getAsJsonPrimitive();
                if(primitive.isString()) {
                    args.put(entry.getKey(), entry.getValue().getAsString());
                } else {
                    args.put(entry.getKey(), entry.getValue().toString());
                }
            } else {
                args.put(entry.getKey(), entry.getValue().toString());
            }
        }

        if(args.isEmpty()) {
            return configJson;
        }

        final Map<String,String> values = new HashMap<>();
        for(final Map.Entry<String,String> entry : args.entrySet()) {
            final String value;
            if(TemplateUtil.isTemplateText(entry.getValue())) {
                value = TemplateUtil.executeStrictTemplate(entry.getValue(), values);
            } else {
                value = entry.getValue();
            }
            values.put("${args." + entry.getKey() + "}", value);
        }

        String configText = configJson.toString();
        for(final Map.Entry<String, String> entry : values.entrySet()) {
            configText = configText.replaceAll(Pattern.quote(entry.getKey()), entry.getValue());
        }
        return new Gson().fromJson(configText, JsonObject.class);
    }

    private static Map<String, Map<String, String>> filterConfigArgs(final String[] args) {
        return Arrays.stream(args)
                .filter(s -> s.contains("=") && s.split("=")[0].contains("."))
                .map(s -> s.startsWith("--") ? s.replaceFirst("--", "") : s)
                .collect(Collectors.groupingBy(
                        s -> s.substring(0, s.indexOf(".")),
                        Collectors.toMap(
                                s -> s.substring(s.indexOf(".") + 1).split("=")[0],
                                s -> s.substring(s.indexOf(".") + 1).split("=", 2)[1],
                                (s1, s2) -> s2)));
    }

    private static JsonObject parseYaml(final String text) {
        final Yaml yaml = new Yaml();
        final Map<?, ?> loadedYaml = yaml.loadAs(text, Map.class);
        final Gson gson = new GsonBuilder().setPrettyPrinting().create();
        final String jsonText = gson.toJson(loadedYaml, Map.class);
        return new Gson().fromJson(jsonText, JsonObject.class);
    }

}
