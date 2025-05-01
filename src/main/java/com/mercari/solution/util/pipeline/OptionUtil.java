package com.mercari.solution.util.pipeline;

import com.google.auth.Credentials;
import com.google.auth.oauth2.*;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSyntaxException;
import com.mercari.solution.MPipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.values.PInput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

public class OptionUtil {

    private static final Logger LOG = LoggerFactory.getLogger(OptionUtil.class);

    public static MPipeline.Runner getRunner(final PipelineOptions options) {
        return switch (options.getRunner().getSimpleName()) {
            case "DirectRunner" -> MPipeline.Runner.direct;
            case "PrismRunner" -> MPipeline.Runner.prism;
            case "PortableRunner" -> MPipeline.Runner.portable;
            case "DataflowRunner" -> MPipeline.Runner.dataflow;
            case "SparkRunner" -> MPipeline.Runner.spark;
            case "FlinkRunner" -> MPipeline.Runner.flink;
            default -> throw new IllegalArgumentException("Not supported runner: " + options.getRunner().getSimpleName());
        };
    }

    public static boolean isStreaming(final PipelineOptions options) {
        return options.as(StreamingOptions.class).isStreaming();
    }

    public static boolean isStreaming(final PInput input) {
        return isStreaming(input.getPipeline().getOptions());
    }

    public static Map<String, Object> getTemplateArgs(final String[] args) {
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

    public static String[] filterPipelineArgs(final String[] args) {
        final List<String> filteredArgs = Arrays.stream(args)
                .filter(s -> !s.contains("=") || !s.split("=")[0].contains("."))
                .toList();
        return filteredArgs.toArray(new String[filteredArgs.size()]);
    }

    public static Map<String, Map<String, String>> filterConfigArgs(final String[] args) {
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

    public static String getDefaultProject() {
        try {
            final Credentials credential = GoogleCredentials.getApplicationDefault();
            return getDefaultProject(credential);
        } catch (final IOException e) {
            throw new IllegalArgumentException("Failed to get default gcp project", e);
        }
    }

    public static String getDefaultProject(final Credentials credential) {
        return switch (credential) {
            case UserCredentials c -> c.getQuotaProjectId();
            case ComputeEngineCredentials c -> c.getQuotaProjectId();
            case ServiceAccountCredentials c -> c.getProjectId();
            case ServiceAccountJwtAccessCredentials c -> c.getQuotaProjectId();
            default -> {
                LOG.error("Not supported credentials: {}, class:{}", credential, credential.getClass().getSimpleName());
                yield null;
            }
        };
    }

    public static String getServiceAccount() {
        try {
            final Credentials credential = GoogleCredentials.getApplicationDefault();
            return getServiceAccount(credential);
        } catch (final IOException e) {
            throw new IllegalArgumentException("Failed to get service account", e);
        }
    }

    public static String getServiceAccount(final Credentials credential) {
        return switch (credential) {
            case UserCredentials c -> c.getClientId();
            case ComputeEngineCredentials c -> c.getAccount();
            case ServiceAccountCredentials c -> c.getServiceAccountUser();
            case ServiceAccountJwtAccessCredentials c -> c.getAccount();
            default -> {
                LOG.error("Not supported credentials: {}, class:{}", credential, credential.getClass().getSimpleName());
                yield null;
            }
        };
    }

    public static boolean isDirectRunner(final PInput input) {
        return PipelineOptions.DirectRunner.class.getSimpleName().equals(input.getPipeline().getOptions().getRunner().getSimpleName());
    }

    public static Set<String> toSet(final List<String> params) {
        return Optional.ofNullable(params)
                .map(HashSet::new)
                .orElse(new HashSet<>());
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

}
