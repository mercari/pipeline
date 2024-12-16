package com.mercari.solution.config;

import com.google.api.services.storage.Storage;
import com.google.gson.*;
import com.mercari.solution.util.TemplateUtil;
import com.mercari.solution.util.gcp.StorageUtil;
import freemarker.template.Template;
import freemarker.template.TemplateException;
import org.joda.time.DateTimeZone;
import org.joda.time.LocalDate;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.io.StringWriter;
import java.util.*;
import java.util.stream.Collectors;


public class Config implements Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(Config.class);

    private static final DateTimeFormatter FORMATTER_YYYYMMDD = DateTimeFormat.forPattern("yyyyMMdd");
    private static final String[] RESERVED_PARAMETERS = {
            "__EVENT_EPOCH_SECOND__",
            "__EVENT_EPOCH_SECOND_PRE__",
            "__EVENT_EPOCH_MILLISECOND__",
            "__EVENT_EPOCH_MILLISECOND_PRE__",
            "__EVENT_DATETIME_ISO__",
            "__EVENT_DATETIME_ISO_PRE__"
    };

    private String name;
    private String description;

    private Map<String, String> args;
    private Options options;
    private List<SourceConfig> sources;
    private List<TransformConfig> transforms;
    private List<SinkConfig> sinks;

    // deprecated
    private List<Import> imports;
    private Options settings;

    private Boolean empty;

    private String content;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getDescription() {
        return description;
    }

    public List<Import> getImports() {
        return imports;
    }

    public void setImports(List<Import> imports) {
        this.imports = imports;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public Map<String, String> getArgs() {
        return args;
    }

    public Options getOptions() {
        return options;
    }

    public Options getSettings() {
        return settings;
    }

    public void setSettings(Options settings) {
        this.settings = settings;
    }

    public List<SourceConfig> getSources() {
        return sources;
    }

    public void setSources(List<SourceConfig> sources) {
        this.sources = sources;
    }

    public List<TransformConfig> getTransforms() {
        return transforms;
    }

    public void setTransforms(List<TransformConfig> transforms) {
        this.transforms = transforms;
    }

    public List<SinkConfig> getSinks() {
        return sinks;
    }

    public void setSinks(List<SinkConfig> sinks) {
        this.sinks = sinks;
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
        final List<String> messages = new ArrayList<>();
        if(this.imports != null) {
            for(final Import i : this.imports) {
                messages.addAll(i.validate());
            }
        }
    }
    public void setDefaults() {
        if(args == null) {
            args = new HashMap<>();
        }
        if(this.imports == null) {
            this.imports = new ArrayList<>();
        } else {
            for(final Import i : this.imports) {
                i.setDefaults();
            }
        }
    }

    public class Import implements Serializable {

        private String name;
        private String base;
        private List<String> files;

        private Map<String, List<String>> inputs;
        private Map<String, JsonObject> parameters;

        private List<String> includes;
        private List<String> excludes;

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getBase() {
            return base;
        }

        public void setBase(String base) {
            this.base = base;
        }

        public List<String> getFiles() {
            return files;
        }

        public void setFiles(List<String> files) {
            this.files = files;
        }

        public Map<String, List<String>> getInputs() {
            return inputs;
        }

        public void setInputs(Map<String, List<String>> inputs) {
            this.inputs = inputs;
        }

        public Map<String, JsonObject> getParameters() {
            return parameters;
        }

        public void setParameters(Map<String, JsonObject> parameters) {
            this.parameters = parameters;
        }

        public List<String> getIncludes() {
            return includes;
        }

        public void setIncludes(List<String> includes) {
            this.includes = includes;
        }

        public List<String> getExcludes() {
            return excludes;
        }

        public void setExcludes(List<String> excludes) {
            this.excludes = excludes;
        }

        public boolean filter(final String name) {
            if(!this.includes.isEmpty()) {
                return this.includes.contains(name);
            }
            if(!this.excludes.isEmpty()) {
                return !this.excludes.contains(name);
            }
            return true;
        }

        public List<String> validate() {
            return new ArrayList<>();
        }

        public void setDefaults() {
            if(this.files == null) {
                this.files = new ArrayList<>();
            }
            if(this.inputs == null) {
                this.inputs = new HashMap<>();
            }
            if(parameters == null) {
                this.parameters = new HashMap<>();
            }
            if(includes == null) {
                this.includes = new ArrayList<>();
            }
            if(excludes == null) {
                this.excludes = new ArrayList<>();
            }
        }
    }

    public static Config parse(final String configJson, final String[] args, final Boolean useConfigTemplate) throws Exception {
        final Map<String, Map<String, String>> argsParameters = filterConfigArgs(args);
        final String templatedConfigJson;
        if(useConfigTemplate) {
            templatedConfigJson = executeTemplate(
                    configJson, argsParameters.getOrDefault("template", new HashMap<>()));
        } else {
            templatedConfigJson = configJson;
        }

        final JsonObject jsonObject;
        try {
            jsonObject= new Gson().fromJson(templatedConfigJson, JsonObject.class);
        } catch (Exception e) {
            final String errorMessage = "Failed to parse template json: " + templatedConfigJson;
            LOG.error(errorMessage);
            throw new IllegalArgumentException(errorMessage, e);
        }

        // Config sources parameters
        if(jsonObject.has("sources")) {
            final JsonElement jsonSources = jsonObject.get("sources");
            if(!jsonSources.isJsonArray()) {
                throw new IllegalArgumentException("Config sources must be array! : " + jsonSources);
            }
            replaceParameters(jsonSources.getAsJsonArray(), argsParameters);
        }

        // Config transforms parameters
        if(jsonObject.has("transforms")) {
            final JsonElement jsonTransforms = jsonObject.get("transforms");
            if(!jsonTransforms.isJsonArray()) {
                throw new IllegalArgumentException("Config transforms must be array! : " + jsonTransforms);
            }
            replaceParameters(jsonTransforms.getAsJsonArray(), argsParameters);
        }

        // Config sinks parameters
        if(jsonObject.has("sinks")) {
            final JsonElement jsonSinks = jsonObject.get("sinks");
            if(!jsonSinks.isJsonArray()) {
                throw new IllegalArgumentException("Config sinks must be array! : " + jsonSinks);
            }
            replaceParameters(jsonSinks.getAsJsonArray(), argsParameters);
        }

        LOG.info("Pipeline config: \n{}", new GsonBuilder().setPrettyPrinting().create().toJson(jsonObject));
        final String jsonText = replaceParameters(jsonObject.toString());
        try {
            final Config config = new Gson().fromJson(jsonText, Config.class);
            if(config == null) {
                throw new IllegalArgumentException("Json Config must not be null !");
            }
            config.setDefaults();

            final Map<String, Object> templateArgs = getTemplateArgs(args);
            final List<SourceConfig> sources = Optional.ofNullable(config.getSources()).orElseGet(ArrayList::new)
                    .stream()
                    .filter(Objects::nonNull)
                    .peek(c -> c.setArgs(templateArgs))
                    .collect(Collectors.toList());
            final List<TransformConfig> transforms = Optional.ofNullable(config.getTransforms()).orElseGet(ArrayList::new)
                    .stream()
                    .filter(Objects::nonNull)
                    .peek(c -> c.setArgs(templateArgs))
                    .collect(Collectors.toList());
            final List<SinkConfig> sinks = Optional.ofNullable(config.getSinks()).orElseGet(ArrayList::new)
                    .stream()
                    .filter(Objects::nonNull)
                    .peek(c -> c.setArgs(templateArgs))
                    .collect(Collectors.toList());

            if(config.getImports() != null && !config.getImports().isEmpty()) {
                final Storage storage = StorageUtil.storage();
                for(final Import i : config.getImports()) {
                    final Map<String, List<String>> inputs = Optional.ofNullable(i.getInputs()).orElseGet(HashMap::new);
                    final Map<String, JsonObject> iparams = Optional.ofNullable(i.getParameters()).orElseGet(HashMap::new);
                    for(final String path : i.getFiles()) {
                        final String gcsPath = (i.getBase() == null ? "" : i.getBase()) + path;
                        final String json = StorageUtil.readString(storage, gcsPath);
                        final Config importConfig = Config.parse(json, args, useConfigTemplate);
                        if(importConfig.getSources() != null) {
                            sources.addAll(importConfig.getSources()
                                    .stream()
                                    .filter(c -> i.filter(c.getName()))
                                    .peek(c -> {
                                        if(iparams.containsKey(c.getName())) {
                                            final JsonObject iparam = iparams.get(c.getName());
                                            setAltParameters(c.getParameters(), iparam);
                                        }
                                    })
                                    .collect(Collectors.toList()));
                        }
                        if(importConfig.getTransforms() != null) {
                            transforms.addAll(importConfig.getTransforms()
                                    .stream()
                                    .filter(c -> i.filter(c.getName()))
                                    .peek(c -> {
                                        if(iparams.containsKey(c.getName())) {
                                            final JsonObject iparam = iparams.get(c.getName());
                                            setAltParameters(c.getParameters(), iparam);
                                        }
                                        if(inputs.containsKey(c.getName())) {
                                            c.setInputs(inputs.get(c.getName()));
                                        }
                                    })
                                    .collect(Collectors.toList()));
                        }
                        if(importConfig.getSinks() != null) {
                            sinks.addAll(importConfig.getSinks()
                                    .stream()
                                    .filter(c -> i.filter(c.getName()))
                                    .peek(c -> {
                                        if(iparams.containsKey(c.getName())) {
                                            final JsonObject iparam = iparams.get(c.getName());
                                            setAltParameters(c.getParameters(), iparam);
                                        }
                                        if(inputs.containsKey(c.getName())) {
                                            final List<String> input = inputs.get(c.getName());
                                            if(!input.isEmpty()) {
                                                c.setInputs(input);
                                            }
                                        }
                                    })
                                    .collect(Collectors.toList()));
                        }
                    }
                }
            }

            if(sources.isEmpty() && transforms.isEmpty() && sinks.isEmpty()) {
                throw new IllegalArgumentException("no module definition!");
            }

            config.setSources(sources);
            config.setTransforms(transforms);
            config.setSinks(sinks);

            config.content = templatedConfigJson;

            return config;
        } catch (Throwable e) {
            throw new RuntimeException("Failed to parse config json: " + jsonText, e);
        }
    }

    static Map<String, Object> getTemplateArgs(final String[] args) {
        final Map<String, Map<String, String>> argsParameters = filterConfigArgs(args);
        final Map<String, Object> map = new HashMap<>();
        for(final Map.Entry<String, String> entry : argsParameters.getOrDefault("template", new HashMap<>()).entrySet()) {
            JsonElement jsonElement;
            try {
                jsonElement = new Gson().fromJson(entry.getValue(), JsonElement.class);
            } catch (final JsonSyntaxException e) {
                jsonElement = new JsonPrimitive(entry.getValue());
            }
            map.put(entry.getKey(), extractTemplateParameters(jsonElement));
        }
        return map;
    }

    private static void replaceParameters(final JsonArray array, final Map<String, Map<String, String>> args) {
        for(final JsonElement element : array) {
            if(element.isJsonNull()) {
                continue;
            }
            if(!element.isJsonObject()) {
                throw new IllegalArgumentException("Config module must be object! -> " + element.toString());
            }
            final JsonObject module = element.getAsJsonObject();
            if(!module.has("name")) {
                throw new IllegalArgumentException("Config module must be set name -> " + module.toString());
            }
            final String name = module.get("name").getAsString();
            if(!args.containsKey(name)) {
                continue;
            }
            if(!module.has("parameters")) {
                module.add("parameters", new JsonObject());
            }

            final JsonObject parameters = module.get("parameters").getAsJsonObject();
            for(final Map.Entry<String, String> arg : args.get(name).entrySet()) {
                parameters.addProperty(arg.getKey(), arg.getValue());
            }
        }
    }

    private static String replaceParameters(final String json) {
        final LocalDate today = LocalDate.now(DateTimeZone.forID("Asia/Tokyo"));
        return json
                .replaceAll("<TODAY>", FORMATTER_YYYYMMDD.print(today))
                .replaceAll("<YESTERDAY>", FORMATTER_YYYYMMDD.print(today.plusDays(-1)));
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

    private static String executeTemplate(final String config, final Map<String, String> parameters) throws IOException, TemplateException {
        final Map<String, Object> map = new HashMap<>();
        for(final String reservedParameter : RESERVED_PARAMETERS) {
            map.put(reservedParameter, String.format("${%s}", reservedParameter));
        }
        for(final Map.Entry<String, String> entry : parameters.entrySet()) {
            JsonElement jsonElement;
            try {
                jsonElement = new Gson().fromJson(entry.getValue(), JsonElement.class);
            } catch (final JsonSyntaxException e) {
                jsonElement = new JsonPrimitive(entry.getValue());
            }
            map.put(entry.getKey(), extractTemplateParameters(jsonElement));
        }

        final Template template = TemplateUtil.createSafeTemplate("config", config);
        final StringWriter stringWriter = new StringWriter();
        template.process(map, stringWriter);
        return stringWriter.toString();
    }

    private static Object extractTemplateParameters(final JsonElement jsonElement) {
        if(jsonElement.isJsonPrimitive()) {
            if(jsonElement.getAsJsonPrimitive().isBoolean()) {
                return jsonElement.getAsBoolean();
            } else if(jsonElement.getAsJsonPrimitive().isString()) {
                return jsonElement.getAsString();
            } else if(jsonElement.getAsJsonPrimitive().isNumber()) {
                return jsonElement.getAsLong();
            }
            return jsonElement.toString();
        }
        if(jsonElement.isJsonObject()) {
            final Map<String, Object> map = new HashMap<>();
            jsonElement.getAsJsonObject().entrySet().forEach(kv -> map.put(kv.getKey(), extractTemplateParameters(kv.getValue())));
            return map;
        }
        if(jsonElement.isJsonArray()) {
            final List<Object> list = new ArrayList<>();
            jsonElement.getAsJsonArray().forEach(element -> list.add(extractTemplateParameters(element)));
            return list;
        }
        return null;
    }

    private static void setAltParameters(final JsonObject parameters, final JsonObject altParameters) {
        for(final Map.Entry<String, JsonElement> entry : altParameters.entrySet()) {
            if(entry.getValue().isJsonObject()) {
                setAltParameters(parameters.get(entry.getKey()).getAsJsonObject(), entry.getValue().getAsJsonObject());
            } else {
                parameters.add(entry.getKey(), entry.getValue());
            }
        }
    }

}
