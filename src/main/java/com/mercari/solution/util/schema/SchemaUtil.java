package com.mercari.solution.util.schema;

import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Type;
import com.google.datastore.v1.Entity;
import com.google.firestore.v1.Document;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.mercari.solution.module.DataType;
import com.mercari.solution.util.schema.converter.*;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Collection;
import java.util.List;
import java.util.Map;


public class SchemaUtil {

    private static final Logger LOG = LoggerFactory.getLogger(SchemaUtil.class);

    public interface DataConverter<OutputSchemaT, InputT, OutputT> extends Serializable {
        OutputT convert(OutputSchemaT schema, InputT input);
    }

    public interface SchemaConverter<InputSchemaT, RuntimeSchemaT> extends Serializable {
        RuntimeSchemaT convert(InputSchemaT schema);
    }

    public interface StringGetter<ElementT> extends Serializable {
        String getAsString(ElementT element, String field);
    }

    public interface BytesGetter<ElementT> extends Serializable {
        byte[] getAsBytes(ElementT element, String field);
    }

    public interface LongGetter<ElementT> extends Serializable {
        Long getAsLong(ElementT element, String field);
    }

    public interface FloatGetter<ElementT> extends Serializable {
        Float getAsFloat(ElementT element, String field);
    }

    public interface DoubleGetter<ElementT> extends Serializable {
        Double getAsDouble(ElementT element, String field);
    }

    public interface FloatListGetter<ElementT> extends  Serializable {
        List<Float> getAsFloats(ElementT element, String field);
    }

    public interface TimestampGetter<ElementT> extends Serializable {
        Instant getAsInstant(ElementT element, String field);
    }

    public interface ValueGetter<ElementT> extends Serializable {
        Object getValue(ElementT element, String field);
    }

    public interface PrimitiveValueGetter extends Serializable {
        Object getValue(Object element, Schema.FieldType fieldType, String field);
    }

    public interface MapConverter<ElementT> extends Serializable {
        Map<String, Object> convert(ElementT element);
    }

    public interface MapFieldsConverter<ElementT> extends Serializable {
        Map<String, Object> convert(ElementT element, Collection<String> fields);
    }

    public interface ValuesSetter<SchemaT, ElementT> extends Serializable {
        ElementT setValues(SchemaT schema, ElementT element, Map<String, ? extends Object> values);
    }

    public interface TimestampConverter extends Serializable {
        Instant toInstant(Object value);
    }

    public interface JsonConverter<SchemaT, ElementT> extends Serializable {
        ElementT convert(SchemaT schema, JsonObject element);
    }

    public interface JsonElementConverter<SchemaT, ElementT> extends Serializable {
        ElementT convert(SchemaT schema, JsonElement json);
    }

    public interface ValueCreator<SchemaT, ElementT> extends Serializable {
        ElementT create(final SchemaT schema, final Map<String, Object> values);
    }

    public interface PrimitiveValueConverter extends Serializable {
        Object convertPrimitive(final Schema.FieldType fieldType, final Object primitiveValue);
    }

    public static <ElementT> SerializableFunction<ElementT, String> createGroupKeysFunction(
            final StringGetter<ElementT> stringGetter, final List<String> groupFields) {

        return (ElementT t) -> {
            final StringBuilder sb = new StringBuilder();
            for(final String fieldName : groupFields) {
                final String fieldValue = stringGetter.getAsString(t, fieldName);
                sb.append(fieldValue == null ? "" : fieldValue);
                sb.append("#");
            }
            if(sb.length() > 0) {
                sb.deleteCharAt(sb.length() - 1);
            }
            return sb.toString();
        };
    }

    public static TupleTag createTupleTag(final DataType dataType) {
        switch (dataType) {
            case ROW:
                return new TupleTag<Row>(){};
            case AVRO:
                return new TupleTag<GenericRecord>(){};
            case STRUCT:
                return new TupleTag<Struct>(){};
            case DOCUMENT:
                return new TupleTag<Document>(){};
            case ENTITY:
                return new TupleTag<Entity>(){};
            case MUTATION:
                return new TupleTag<Mutation>(){};
            default:
                throw new IllegalArgumentException();
        }
    }

    public static TypeTransform transform(
            final Object outputSchema,
            final DataType inputType,
            final DataType outputType) {

        switch (inputType) {
            case AVRO: {
                switch (outputType) {
                    case AVRO: {
                        final org.apache.avro.Schema outputAvroSchema = (org.apache.avro.Schema) outputSchema;
                        return new TypeTransform<String, org.apache.avro.Schema,GenericRecord,GenericRecord>(outputAvroSchema.toString(), AvroSchemaUtil::convertSchema, (s, r) -> r, AvroCoder.of(outputAvroSchema));
                    }
                    case ROW: {
                        final Schema outputRowSchema = (Schema) outputSchema;
                        return new TypeTransform<Schema,Schema,GenericRecord,Row>(outputRowSchema, s -> s, AvroToRowConverter::convert, RowCoder.of(outputRowSchema));
                    }
                    default:
                        throw new IllegalArgumentException("Not supported conversion from: " + inputType + " to: " + outputType);
                }
            }
            case ROW: {
                switch (outputType) {
                    case AVRO: {
                        final org.apache.avro.Schema outputAvroSchema = (org.apache.avro.Schema) outputSchema;
                        return new TypeTransform<String,org.apache.avro.Schema,Row,GenericRecord>(outputAvroSchema.toString(), AvroSchemaUtil::convertSchema, RowToRecordConverter::convert, AvroCoder.of(outputAvroSchema));
                    }
                    case ROW: {
                        final Schema outputRowSchema = (Schema) outputSchema;
                        return new TypeTransform<Schema,Schema,Row,Row>(outputRowSchema, s -> s, (s, r) -> r, RowCoder.of(outputRowSchema));
                    }
                    default:
                        throw new IllegalArgumentException("Not supported conversion from: " + inputType + " to: " + outputType);
                }
            }
            case STRUCT: {
                switch (outputType) {
                    case AVRO: {
                        final org.apache.avro.Schema outputAvroSchema = (org.apache.avro.Schema) outputSchema;
                        return new TypeTransform<String,org.apache.avro.Schema,Struct,GenericRecord>(outputAvroSchema.toString(), AvroSchemaUtil::convertSchema, StructToAvroConverter::convert, AvroCoder.of(outputAvroSchema));
                    }
                    case ROW: {
                        final Schema outputRowSchema = (Schema) outputSchema;
                        return new TypeTransform<Schema,Schema,Struct,Row>(outputRowSchema, s -> s, StructToRowConverter::convert, RowCoder.of(outputRowSchema));
                    }
                    case STRUCT: {
                        final Type outputTypeSchema = (Type) outputSchema;
                        return new TypeTransform<Type,Type,Struct,Struct>(outputTypeSchema, s -> s, (s, r) -> r, SerializableCoder.of(Struct.class));
                    }
                    default:
                        throw new IllegalArgumentException("Not supported conversion from: " + inputType + " to: " + outputType);
                }
            }
            case DOCUMENT: {
                switch (outputType) {
                    case AVRO: {
                        final org.apache.avro.Schema outputAvroSchema = (org.apache.avro.Schema) outputSchema;
                        return new TypeTransform<String, org.apache.avro.Schema, Document, GenericRecord>(outputAvroSchema.toString(), AvroSchemaUtil::convertSchema, DocumentToAvroConverter::convert, AvroCoder.of(outputAvroSchema));
                    }
                    case ROW: {
                        final Schema outputRowSchema = (Schema) outputSchema;
                        return new TypeTransform<>(outputRowSchema, s -> s, DocumentToRowConverter::convert, RowCoder.of(outputRowSchema));
                    }
                    case DOCUMENT: {
                        final Schema outputRowSchema = (Schema) outputSchema;
                        return new TypeTransform<Schema,Schema,Document,Document>(outputRowSchema, s -> s, (s, r) -> r, SerializableCoder.of(Document.class));
                    }
                    default:
                        throw new IllegalArgumentException("Not supported conversion from: " + inputType + " to: " + outputType);
                }
            }
            case ENTITY: {
                switch (outputType) {
                    case AVRO: {
                        final org.apache.avro.Schema outputAvroSchema = (org.apache.avro.Schema) outputSchema;
                        return new TypeTransform<String, org.apache.avro.Schema,Entity,GenericRecord>(outputAvroSchema.toString(), AvroSchemaUtil::convertSchema, EntityToAvroConverter::convert, AvroCoder.of(outputAvroSchema));
                    }
                    case ROW: {
                        final Schema outputRowSchema = (Schema) outputSchema;
                        return new TypeTransform<>(outputRowSchema, s -> s, EntityToRowConverter::convert, RowCoder.of(outputRowSchema));
                    }
                    case ENTITY: {
                        final Schema outputRowSchema = (Schema) outputSchema;
                        return new TypeTransform<Schema,Schema,Entity,Entity>(outputRowSchema, s -> s, (s, r) -> r, SerializableCoder.of(Entity.class));
                    }
                    default:
                        throw new IllegalArgumentException("Not supported conversion from: " + inputType + " to: " + outputType);
                }
            }
            default:
                throw new IllegalArgumentException("Not supported conversion from: " + inputType + " to: " + outputType);
        }
    }


    private static class TypeTransform<OutputSchemaT,RuntimeOutputSchemaT,InputT,OutputT> extends PTransform<PCollection<InputT>, PCollection<OutputT>> {

        private final OutputSchemaT inputOutputSchema;
        private final SchemaConverter<OutputSchemaT, RuntimeOutputSchemaT> schemaConverter;
        private final DataConverter<RuntimeOutputSchemaT, InputT, OutputT> dataConverter;
        private final Coder<OutputT> coder;

        private TypeTransform(final OutputSchemaT inputOutputSchema,
                              final SchemaConverter<OutputSchemaT, RuntimeOutputSchemaT> schemaConverter,
                              final DataConverter<RuntimeOutputSchemaT, InputT, OutputT> dataConverter,
                              final Coder<OutputT> coder) {

            this.inputOutputSchema = inputOutputSchema;
            this.schemaConverter = schemaConverter;
            this.dataConverter = dataConverter;
            this.coder = coder;
        }

        @Override
        public PCollection<OutputT> expand(final PCollection<InputT> input) {
            return input
                    .apply("ConvertDataType", ParDo
                            .of(new ConvertDoFn<>(
                                    inputOutputSchema,
                                    schemaConverter,
                                    dataConverter)))
                    .setCoder(coder);
        }

        private static class ConvertDoFn<OutputSchemaT, RuntimeOutputSchemaT, InputT, OutputT> extends DoFn<InputT, OutputT> {

            private final OutputSchemaT schema;
            private final SchemaConverter<OutputSchemaT, RuntimeOutputSchemaT> schemaConverter;
            private final DataConverter<RuntimeOutputSchemaT, InputT, OutputT> dataConverter;

            private transient RuntimeOutputSchemaT runtimeSchema;

            private ConvertDoFn(final OutputSchemaT schema,
                                final SchemaConverter<OutputSchemaT, RuntimeOutputSchemaT> schemaConverter,
                                final DataConverter<RuntimeOutputSchemaT, InputT, OutputT> dataConverter) {

                this.schema = schema;
                this.schemaConverter = schemaConverter;
                this.dataConverter = dataConverter;
            }

            @Setup
            public void setup() {
                this.runtimeSchema = this.schemaConverter.convert(this.schema);
            }

            @ProcessElement
            public void processElement(ProcessContext c) {
                final InputT input = c.element();
                final OutputT output = this.dataConverter.convert(runtimeSchema, input);
                c.output(output);
            }
        }

    }

}
