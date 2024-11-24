package com.mercari.solution.util.domain.text;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import org.apache.beam.vendor.calcite.v1_28_0.com.jayway.jsonpath.Configuration;
import org.apache.beam.vendor.calcite.v1_28_0.com.jayway.jsonpath.DocumentContext;
import org.apache.beam.vendor.calcite.v1_28_0.com.jayway.jsonpath.JsonPath;
import org.apache.beam.vendor.calcite.v1_28_0.com.jayway.jsonpath.Option;
import org.apache.beam.vendor.calcite.v1_28_0.com.jayway.jsonpath.spi.json.JacksonJsonProvider;
import org.apache.beam.vendor.calcite.v1_28_0.com.jayway.jsonpath.spi.json.JsonProvider;
import org.apache.beam.vendor.calcite.v1_28_0.com.jayway.jsonpath.spi.mapper.JacksonMappingProvider;
import org.apache.beam.vendor.calcite.v1_28_0.com.jayway.jsonpath.spi.mapper.MappingProvider;
import org.apache.beam.sdk.values.KV;

import java.util.*;

public class JsonUtil {

    private static final Configuration.Defaults CONF_DEFAULT = new Configuration.Defaults() {
        private final JsonProvider jsonProvider = new JacksonJsonProvider();
        private final MappingProvider mappingProvider = new JacksonMappingProvider();

        @Override
        public JsonProvider jsonProvider() {
            return jsonProvider;
        }

        @Override
        public MappingProvider mappingProvider() {
            return mappingProvider;
        }

        @Override
        public Set<Option> options() {
            return EnumSet.noneOf(Option.class);
        }
    };

    private static final Configuration CONF_PATH = Configuration.builder()
            .jsonProvider(CONF_DEFAULT.jsonProvider())
            .mappingProvider(CONF_DEFAULT.mappingProvider())
            .options(Option.AS_PATH_LIST)
            .build();

    public static JsonElement fromJson(final String json) {
        return new Gson().fromJson(json, JsonElement.class);
    }

    public static List<KV<String, Object>> read(JsonObject json, String jsonPath) {
        return read(json.toString(), jsonPath);
    }

    public static List<KV<String, Object>> read(String json, String jsonPath) {

        Configuration.setDefaults(CONF_DEFAULT);

        final DocumentContext dc = JsonPath
                .using(CONF_PATH)
                .parse(json);

        final List<String> paths;
        try {
            paths = dc.read(jsonPath);
        } catch (Throwable e) {
            return new ArrayList<>();
        }
        var values = new ArrayList<KV<String, Object>>();
        for(var path : paths) {
            final Object value = JsonPath.read(json, path);
            values.add(KV.of(path, value));
        }
        return values;
    }

    public static Object getJsonPathValue(String json, String jsonPath) {

        Configuration.setDefaults(CONF_DEFAULT);

        final DocumentContext dc = JsonPath
                .using(CONF_PATH)
                .parse(json);

        final List<String> paths;
        try {
            paths = dc.read(jsonPath);
        } catch (Throwable e) {
            return null;
        }

        if(paths == null || paths.isEmpty()) {
            return null;
        } else {
            return JsonPath.read(json, paths.getFirst());
        }
    }

    public static JsonObject set(JsonObject json, List<KV<String, Object>> pathAndValues) {
        final String result = set(json.toString(), pathAndValues);
        return new Gson().fromJson(result, JsonObject.class);
    }

    public static String set(String json, List<KV<String, Object>> pathAndValues) {
        Configuration.setDefaults(CONF_DEFAULT);

        final DocumentContext dc = JsonPath.parse(json);

        for(var pathAndValue : pathAndValues) {
            dc.set(pathAndValue.getKey(), pathAndValue.getValue());
        }

        return dc.jsonString();
    }

}
