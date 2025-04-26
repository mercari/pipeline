package com.mercari.solution.module;

import com.google.common.reflect.ClassPath;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.mercari.solution.config.Config;
import com.mercari.solution.config.FailureConfig;
import com.mercari.solution.config.ModuleConfig;
import com.mercari.solution.util.DateTimeUtil;
import com.mercari.solution.util.schema.AvroSchemaUtil;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.errorhandling.BadRecord;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.joda.time.Instant;

import java.io.IOException;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.InvocationTargetException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.stream.Collectors;

public abstract class FailureSink extends PTransform<PCollection<BadRecord>, PDone> {

    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.TYPE)
    public @interface Module {
        String name();
    }

    private static final Map<String, Class<FailureSink>> failures = findFailuresInPackage("com.mercari.solution.module.failure");

    private static final String AVRO_SCHEMA_BAD_RECORD = """
            {
              "type" : "record",
              "name" : "BadRecord",
              "fields" : [
                {
                  "name" : "job",
                  "type" : "string",
                  "order" : "ignore"
                },
                {
                  "name" : "module",
                  "type" : "string",
                  "order" : "ignore"
                },
                {
                  "name" : "record",
                  "type" : [
                    {
                      "type" : "record",
                      "name" : "Record",
                      "fields" : [
                        {
                          "name" : "coder",
                          "type" : [ "string", "null" ],
                          "order" : "ignore"
                        },
                        {
                          "name" : "json",
                          "type" : [ "string", "null" ],
                          "order" : "ignore"
                        },
                        {
                          "name" : "bytes",
                          "type" : [ "bytes", "null" ],
                          "order" : "ignore"
                        }
                      ]
                    }, "null" ],
                  "order" : "ignore"
                },
                {
                  "name" : "failure",
                  "type" : [
                    {
                      "type" : "record",
                      "name" : "failure",
                      "fields" : [
                        {
                          "name" : "description",
                          "type" : [ "string", "null" ],
                          "order" : "ignore"
                        },
                        {
                          "name" : "exception",
                          "type" : [ "string", "null" ],
                          "order" : "ignore"
                        },
                        {
                          "name" : "stacktrace",
                          "type" : [ "string", "null" ],
                          "order" : "ignore"
                        }
                      ]
                    }, "null" ],
                  "order" : "ignore"
                },
                {
                  "name" : "timestamp",
                  "type" : {
                    "type" : "long",
                    "logicalType" : "timestamp-micros"
                  },
                  "order" : "ignore"
                },
                {
                  "name" : "eventtime",
                  "type" : {
                    "type" : "long",
                    "logicalType" : "timestamp-micros"
                  },
                  "order" : "ignore"
                }
              ]
            }
            """;

    protected String name;
    protected String module;

    protected String jobName;
    protected String moduleName;

    protected List<Logging> loggings;

    protected Map<String, String> templateArgs;

    protected Strategy strategy;

    private Set<String> tags;
    private String parametersText;

    private void setup(
            final @NonNull FailureConfig config,
            final @NonNull String moduleName,
            final FailureSink.Module properties,
            final PipelineOptions options) {

        this.name = config.getName();
        this.module = config.getModule();
        this.jobName = options.getJobName();
        this.moduleName = moduleName;
        this.parametersText = config.getParameters().toString();

        this.tags = Optional.ofNullable(config.getTags()).orElseGet(HashSet::new);
        this.loggings = Optional
                .ofNullable(config.getLoggings())
                .map(l -> {
                    l.forEach(ll -> ll.setModuleName(name));
                    return l;
                })
                .orElseGet(ArrayList::new);

        this.templateArgs = config.getArgs();
        this.strategy = Optional
                .ofNullable(config.getStrategy())
                .orElseGet(Strategy::createDefaultStrategy);
    }

    public static @NonNull FailureSink create(
            final @NonNull FailureConfig failureConfig,
            final @NonNull String moduleName,
            final @NonNull PipelineOptions options) {

        return Optional.ofNullable(failures.get(failureConfig.getModule())).map(clazz -> {
            final FailureSink module;
            try {
                module = clazz.getDeclaredConstructor().newInstance();
            } catch (InstantiationException | IllegalAccessException | NoSuchMethodException |
                     InvocationTargetException e) {
                throw new IllegalModuleException("Failed to instantiate sinks module: " + failureConfig.getModule() + ", class: " + clazz, e);
            }
            final FailureSink.Module properties = module.getClass().getAnnotation(FailureSink.Module.class);
            module.setup(failureConfig, moduleName, properties, options);
            return module;
        }).orElseThrow(() -> new IllegalArgumentException("Not supported sinks module: " + failureConfig.getModule()));
    }

    private static Map<String, Class<FailureSink>> findFailuresInPackage(String packageName) {
        final ClassPath classPath;
        try {
            ClassLoader loader = FailureSink.class.getClassLoader();
            classPath = ClassPath.from(loader);
        } catch (final IOException ioe) {
            throw new RuntimeException("Reading classpath resource failed", ioe);
        }
        return classPath.getTopLevelClassesRecursive(packageName)
                .stream()
                .map(ClassPath.ClassInfo::load)
                .filter(FailureSink.class::isAssignableFrom)
                .map(clazz -> (Class<FailureSink>)clazz.asSubclass(FailureSink.class))
                .peek(clazz -> {
                    if(!clazz.isAnnotationPresent(FailureSink.Module.class)) {
                        throw new IllegalModuleException("failure sink module: " + clazz.getName() + " must have Module.Failure annotation");
                    }
                })
                .collect(Collectors.toMap(
                        c -> c.getAnnotation(FailureSink.Module.class).name(),
                        c -> c));
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

    public static Schema createBadRecordSchema() {
        return Schema.builder()
                .withField("job", Schema.FieldType.STRING.withNullable(false))
                .withField("module", Schema.FieldType.STRING.withNullable(false))
                .withField("record", Schema.FieldType.element(Schema.builder()
                        .withField("coder", Schema.FieldType.STRING)
                        .withField("json", Schema.FieldType.STRING)
                        .withField("bytes", Schema.FieldType.BYTES)
                        .build()))
                .withField("failure", Schema.FieldType.element(Schema.builder()
                        .withField("description", Schema.FieldType.STRING)
                        .withField("exception", Schema.FieldType.STRING)
                        .withField("stacktrace", Schema.FieldType.STRING)
                        .build()))
                .withField("timestamp", Schema.FieldType.TIMESTAMP.withNullable(false))
                .withField("eventtime", Schema.FieldType.TIMESTAMP.withNullable(false))
                .build();
    }

    public static org.apache.avro.Schema createBadRecordAvroSchema() {
        return AvroSchemaUtil.convertSchema(AVRO_SCHEMA_BAD_RECORD);
    }

    public static Map<String, Object> convertToMap(
            final BadRecord badRecord,
            final String jobName,
            final String moduleName,
            final Instant eventTime) {

        final Map<String, Object> values = new HashMap<>();
        values.put("job", jobName);
        values.put("module", moduleName);
        {
            final Map<String, Object> recordValues = new HashMap<>();
            if(badRecord.getRecord() != null) {
                recordValues.put("coder", badRecord.getRecord().getCoder());
                recordValues.put("json", badRecord.getRecord().getHumanReadableJsonRecord());
                recordValues.put("bytes", Optional
                        .ofNullable(badRecord.getRecord().getEncodedRecord())
                        .map(ByteBuffer::wrap)
                        .orElse(null));
            }
            values.put("record", recordValues);
        }
        {
            final Map<String, Object> failureValues = new HashMap<>();
            if(badRecord.getFailure() != null) {
                failureValues.put("description", badRecord.getFailure().getDescription());
                failureValues.put("exception", badRecord.getFailure().getException());
                failureValues.put("stacktrace", badRecord.getFailure().getExceptionStacktrace());
            }
            values.put("failure", failureValues);
        }
        values.put("timestamp", DateTimeUtil.toEpochMicroSecond(java.time.Instant.now()));
        values.put("eventtime", eventTime.getMillis() * 1000L);
        return values;
    }

    public static GenericRecord convertToAvro(
            final org.apache.avro.Schema schema,
            final BadRecord badRecord,
            final String jobName,
            final String moduleName,
            final Instant eventTime) {

        final Map<String, Object> values = convertToMap(badRecord, jobName, moduleName, eventTime);
        return AvroSchemaUtil.create(schema,values);
    }

    public static FailureSinks merge(final List<FailureSink> sinks) {
        return new FailureSinks(sinks);
    }

    public static class FailureSinks extends PTransform<PCollection<BadRecord>, PDone> {

        private transient List<FailureSink> sinks;

        FailureSinks(final List<FailureSink> sinks) {
            this.sinks = sinks;
        }

        @Override
        public PDone expand(PCollection<BadRecord> input) {
            final Pipeline pipeline = input.getPipeline();
            for(final FailureSink sink : sinks) {
                input.apply(sink);
            }
            return PDone.in(pipeline);
        }
    }

}
