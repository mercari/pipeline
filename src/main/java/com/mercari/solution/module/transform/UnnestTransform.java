package com.mercari.solution.module.transform;

import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Type;
import com.google.gson.Gson;
import com.mercari.solution.config.TransformConfig;
import com.mercari.solution.module.DataType;
import com.mercari.solution.module.FCollection;
import com.mercari.solution.util.schema.RowSchemaUtil;
import com.mercari.solution.util.schema.StructSchemaUtil;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class UnnestTransform {

    private static final Logger LOG = LoggerFactory.getLogger(UnnestTransform.class);

    private class UnnestTransformParameters implements Serializable {

        private String path;
        private Boolean prefix;

        public String getPath() {
            return path;
        }

        public void setPath(String path) {
            this.path = path;
        }

        public Boolean getPrefix() {
            return prefix;
        }

        public void setPrefix(Boolean prefix) {
            this.prefix = prefix;
        }
    }

    public String getName() { return "unnest"; }

    public Map<String, FCollection<?>> expand(List<FCollection<?>> inputs, TransformConfig config) {
        return UnnestTransform.transform(inputs, config);
    }

    public static Map<String, FCollection<?>> transform(final List<FCollection<?>> inputs, final TransformConfig config) {

        final UnnestTransformParameters parameters = new Gson().fromJson(config.getParameters(), UnnestTransformParameters.class);
        validateParameters(parameters);
        setDefaultParameters(parameters);

        final Map<String, FCollection<?>> results = new HashMap<>();
        for(FCollection<?> input : inputs) {
            final String name = config.getName() + (config.getInputs().size() == 1 ? "" : "." + input.getName());
            switch (input.getDataType()) {
                case AVRO:
                    // TODO
                    throw new IllegalArgumentException("UnnestTransform does not yet support input type: Avro");
                case ROW: {
                    final Schema outputSchema = RowSchemaUtil.flatten(input.getSchema(),
                            parameters.getPath(), parameters.getPrefix());
                    final FCollection<Row> inputCollection = (FCollection<Row>) input;
                    final Flatten<Row, Schema, Schema> transform = new Flatten<>(
                            parameters,
                            outputSchema,
                            s -> s,
                            RowSchemaUtil::flatten);
                    final PCollection<Row> output = inputCollection.getCollection()
                            .apply(name, transform)
                            .setCoder(RowCoder.of(outputSchema));
                    results.put(name, FCollection.of(name, output, DataType.ROW, outputSchema));
                    break;
                }
                case STRUCT:
                    final Type outputType = StructSchemaUtil.flatten(input.getSpannerType(),
                            parameters.getPath(), parameters.getPrefix());
                    final FCollection<Struct> inputCollection = (FCollection<Struct>) input;
                    final Flatten<Struct, Type, Type> transform = new Flatten<>(
                            parameters,
                            outputType,
                            s -> s,
                            StructSchemaUtil::flatten);
                    final PCollection<Struct> output = inputCollection.getCollection()
                            .apply(name, transform);
                    results.put(name, FCollection.of(name, output, DataType.STRUCT, outputType));
                    break;
                case ENTITY:
                    // TODO
                    throw new IllegalArgumentException("UnnestTransform does not yet support input type: Datastore Entity");
                default: {
                    throw new IllegalArgumentException("UnnestTransform does not support input type: " + input.getDataType());
                }
            }
        }

        return results;
    }

    private static void validateParameters(final UnnestTransformParameters parameters) {
        if(parameters == null) {
            throw new IllegalArgumentException("UnnestTransform config parameters must not be empty!");
        }

        if(parameters.getPath() == null) {
            throw new IllegalArgumentException("UnnestTransform config parameters must contain path parameter.");
        }
    }

    private static void setDefaultParameters(final UnnestTransformParameters parameters) {
        if(parameters.getPrefix() == null) {
            parameters.setPrefix(true);
        }
    }


    public static class Flatten<T, InputSchemaT, RuntimeSchemaT> extends PTransform<PCollection<T>, PCollection<T>> {

        private final UnnestTransformParameters parameters;
        private final InputSchemaT inputSchema;
        private final SerializableFunction<InputSchemaT, RuntimeSchemaT> schemaConverter;
        private final ValueFlatten<RuntimeSchemaT, T> valueFlatten;

        private Flatten(final UnnestTransformParameters parameters,
                        final InputSchemaT schema,
                        final SerializableFunction<InputSchemaT, RuntimeSchemaT> schemaConverter,
                        final ValueFlatten<RuntimeSchemaT, T> valueFlatten) {

            this.parameters = parameters;
            this.inputSchema = schema;
            this.schemaConverter = schemaConverter;
            this.valueFlatten = valueFlatten;
            validate();
        }

        @Override
        public PCollection<T> expand(final PCollection<T> input) {
            return input.apply("Flatten", ParDo.of(new FlattenDoFn(
                    parameters.getPath(), parameters.getPrefix(),
                    inputSchema, schemaConverter, valueFlatten)));
        }

        private void validate() {
            if(this.parameters == null && this.parameters.getPath() == null) {
                //throw new IllegalArgumentException("Flatten module required dataType parameter!");
            }
        }

        private class FlattenDoFn extends DoFn<T, T> {

            private final String path;
            private final boolean prefix;
            private final InputSchemaT inputSchema;
            private final SerializableFunction<InputSchemaT, RuntimeSchemaT> schemaConverter;
            private final ValueFlatten<RuntimeSchemaT, T> valueFlatten;

            private transient RuntimeSchemaT schema;

            FlattenDoFn(final String path,
                        final boolean prefix,
                        final InputSchemaT schema,
                        final SerializableFunction<InputSchemaT, RuntimeSchemaT> schemaConverter,
                        final ValueFlatten<RuntimeSchemaT, T> valueFlatten) {

                this.path = path;
                this.prefix = prefix;
                this.inputSchema = schema;
                this.schemaConverter = schemaConverter;
                this.valueFlatten = valueFlatten;
            }

            @Setup
            public void setup() {
                this.schema = this.schemaConverter.apply(inputSchema);
            }

            @ProcessElement
            public void processElement(final ProcessContext c) {
                final T element = c.element();
                final List<T> outputs = this.valueFlatten.flatten(this.schema, element, path, prefix);
                outputs.forEach(c::output);
            }

        }

    }

    private interface ValueFlatten<SchemaT, T> extends Serializable {
        List<T> flatten(final SchemaT schema, final T element, final String path, final boolean prefix);
    }

}