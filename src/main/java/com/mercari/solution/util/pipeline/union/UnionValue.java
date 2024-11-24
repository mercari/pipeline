package com.mercari.solution.util.pipeline.union;

import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Type;
import com.google.datastore.v1.Entity;
import com.google.firestore.v1.Document;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.mercari.solution.module.DataType;
import com.mercari.solution.util.pipeline.mutation.UnifiedMutation;
import com.mercari.solution.util.schema.*;
import com.mercari.solution.util.schema.converter.*;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.extensions.avro.coders.AvroGenericCoder;
import org.apache.beam.sdk.io.gcp.spanner.MutationGroup;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.Row;

import java.util.*;

public class UnionValue {

    private final int index;
    private final DataType type;
    private final long epochMillis;
    private final Object value;

    public UnionValue(int index, DataType dataType, long epochMillis, Object value) {
        this.index = index;
        this.type = dataType;
        this.epochMillis = epochMillis;
        this.value = value;
    }

    public int getIndex() {
        return index;
    }

    public DataType getType() {
        return type;
    }

    public long getEpochMillis() {
        return epochMillis;
    }

    public Object getValue() {
        return value;
    }

    @Override
    public boolean equals(Object o) {
        if(this == o) {
            return true;
        }
        if(o == null || getClass() != o.getClass()) {
            return false;
        }

        final UnionValue that = (UnionValue) o;

        if(index != that.index) {
            return false;
        }
        if(epochMillis != that.epochMillis) {
            return false;
        }
        return Objects.equals(value, that.value);

    }

    @Override
    public int hashCode() {
        return 31 * index + (value != null ? value.hashCode() : 0);
    }

    public static Object getFieldValue(final UnionValue unionValue, final String field) {
        if(unionValue.value == null) {
            return null;
        }
        switch (unionValue.type) {
            case ROW -> {
                final Row row = (Row) unionValue.value;
                return RowSchemaUtil.getValue(row, field);
            }
            case AVRO -> {
                final GenericRecord record = (GenericRecord) unionValue.value;
                return AvroSchemaUtil.getValue(record, field);
            }
            case STRUCT -> {
                final Struct struct = (Struct) unionValue.value;
                return StructSchemaUtil.getValue(struct, field);
            }
            case DOCUMENT -> {
                final Document document = (Document) unionValue.value;
                return DocumentSchemaUtil.getValue(document, field);
            }
            case ENTITY -> {
                final Entity entity = (Entity) unionValue.value;
                return EntitySchemaUtil.getValue(entity, field);
            }
            default -> throw new IllegalStateException("Union not supported data type: " + unionValue.type.name());
        }
    }

    public String getString(final String field) {
        return getAsString(this, field);
    }
    public Double getDouble(final String field) {
        return getAsDouble(this, field);
    }

    public Map<String, Object> getMap() {
        return getAsMap(this, null);
    }

    public Map<String, Object> getMap(final Collection<String> fields) {
        return getAsMap(this, fields);
    }

    public Map<String, Double> getDoubleMap(final Collection<String> fields) {
        return getAsDoubleMap(this, fields);
    }

    public static String getAsString(final UnionValue unionValue, final String field) {
        if(unionValue.value == null) {
            return null;
        }
        switch (unionValue.type) {
            case ROW -> {
                final Row row = (Row) unionValue.value;
                return RowSchemaUtil.getAsString(row, field);
            }
            case AVRO -> {
                final GenericRecord record = (GenericRecord) unionValue.value;
                return AvroSchemaUtil.getAsString(record, field);
            }
            case STRUCT -> {
                final Struct struct = (Struct) unionValue.value;
                return StructSchemaUtil.getAsString(struct, field);
            }
            case DOCUMENT -> {
                final Document document = (Document) unionValue.value;
                return DocumentSchemaUtil.getAsString(document, field);
            }
            case ENTITY -> {
                final Entity entity = (Entity) unionValue.value;
                return EntitySchemaUtil.getAsString(entity, field);
            }
            default -> throw new IllegalStateException("Union not supported data type: " + unionValue.type.name());
        }
    }

    public static Double getAsDouble(final UnionValue unionValue, final String field) {
        if(unionValue.value == null) {
            return null;
        }
        switch (unionValue.type) {
            case ROW -> {
                final Row row = (Row) unionValue.value;
                return RowSchemaUtil.getAsDouble(row, field);
            }
            case AVRO -> {
                final GenericRecord record = (GenericRecord) unionValue.value;
                return AvroSchemaUtil.getAsDouble(record, field);
            }
            case STRUCT -> {
                final Struct struct = (Struct) unionValue.value;
                return StructSchemaUtil.getAsDouble(struct, field);
            }
            case DOCUMENT -> {
                final Document document = (Document) unionValue.value;
                return DocumentSchemaUtil.getAsDouble(document, field);
            }
            case ENTITY -> {
                final Entity entity = (Entity) unionValue.value;
                return EntitySchemaUtil.getAsDouble(entity, field);
            }
            default -> throw new IllegalStateException("Union not supported data type: " + unionValue.type.name());
        }

    }

    public static Map<String, Object> getAsMap(final UnionValue unionValue, final Collection<String> fields) {
        if(unionValue.value == null) {
            return new HashMap<>();
        }
        switch (unionValue.type) {
            case ROW -> {
                final Row row = (Row) unionValue.value;
                return RowToMapConverter.convertWithFields(row, fields);
            }
            case AVRO -> {
                final GenericRecord record = (GenericRecord) unionValue.value;
                return AvroToMapConverter.convertWithFields(record, fields);
            }
            case STRUCT -> {
                final Struct struct = (Struct) unionValue.value;
                return StructToMapConverter.convertWithFields(struct, fields);
            }
            case DOCUMENT -> {
                final Document document = (Document) unionValue.value;
                return DocumentToMapConverter.convertWithFields(document, fields);
            }
            case ENTITY -> {
                final Entity entity = (Entity) unionValue.value;
                return EntityToMapConverter.convertWithFields(entity, fields);
            }
            default -> throw new IllegalStateException("Union not supported data type: " + unionValue.type.name());
        }
    }

    public static Map<String, Object> asPrimitiveMap(final UnionValue unionValue) {
        if(unionValue.value == null) {
            return new HashMap<>();
        }
        switch (unionValue.type) {
            case ROW -> {
                final Row row = (Row) unionValue.value;
                return RowSchemaUtil.asPrimitiveMap(row);
            }
            case AVRO -> {
                final GenericRecord record = (GenericRecord) unionValue.value;
                return AvroSchemaUtil.asPrimitiveMap(record);
            }
            case STRUCT -> {
                final Struct struct = (Struct) unionValue.value;
                return StructSchemaUtil.asPrimitiveMap(struct);
            }
            case DOCUMENT -> {
                final Document document = (Document) unionValue.value;
                return DocumentSchemaUtil.asPrimitiveMap(document);
            }
            case ENTITY -> {
                final Entity entity = (Entity) unionValue.value;
                return EntitySchemaUtil.asPrimitiveMap(entity);
            }
            default -> throw new IllegalStateException("Union not supported data type: " + unionValue.type.name());
        }
    }

    public static Map<String, Double> getAsDoubleMap(final UnionValue unionValue, final Collection<String> fields) {
        final Map<String, Double> doubles = new HashMap<>();
        if(unionValue.value == null) {
            return doubles;
        }
        switch (unionValue.type) {
            case ROW -> {
                final Row row = (Row) unionValue.value;
                for (final String field : fields) {
                    doubles.put(field, RowSchemaUtil.getAsDouble(row, field));
                }
            }
            case AVRO -> {
                final GenericRecord record = (GenericRecord) unionValue.value;
                for (final String field : fields) {
                    doubles.put(field, AvroSchemaUtil.getAsDouble(record, field));
                }
            }
            case STRUCT -> {
                final Struct struct = (Struct) unionValue.value;
                for (final String field : fields) {
                    doubles.put(field, StructSchemaUtil.getAsDouble(struct, field));
                }
            }
            case DOCUMENT -> {
                final Document document = (Document) unionValue.value;
                for (final String field : fields) {
                    doubles.put(field, DocumentSchemaUtil.getAsDouble(document, field));
                }
            }
            case ENTITY -> {
                final Entity entity = (Entity) unionValue.value;
                for (final String field : fields) {
                    doubles.put(field, EntitySchemaUtil.getAsDouble(entity, field));
                }
            }
            default -> throw new IllegalStateException("Union not supported data type: " + unionValue.type.name());
        }

        return doubles;
    }

    public static String getAsCsvLine(final UnionValue unionValue, final List<String> fields) {
        if(unionValue == null) {
            return null;
        }
        return switch (unionValue.getType()) {
            case ROW -> RowToCsvConverter.convert((Row) unionValue.getValue(), fields);
            case AVRO -> AvroToCsvConverter.convert((GenericRecord) unionValue.getValue(), fields);
            case STRUCT -> StructToCsvConverter.convert((Struct) unionValue.getValue(), fields);
            case DOCUMENT -> DocumentToCsvConverter.convert((Document) unionValue.getValue(), fields);
            case ENTITY -> EntityToCsvConverter.convert((Entity) unionValue.getValue(), fields);
            default -> throw new IllegalArgumentException("Not supported csv conversion: " + unionValue.getType());
        };
    }

    public static String getAsJson(final UnionValue unionValue) {
        if(unionValue == null) {
            return null;
        }
        return switch (unionValue.getType()) {
            case ROW -> RowToJsonConverter.convert((Row) unionValue.getValue());
            case AVRO -> AvroToJsonConverter.convert((GenericRecord) unionValue.getValue());
            case STRUCT -> StructToJsonConverter.convert((Struct) unionValue.getValue());
            case DOCUMENT -> DocumentToJsonConverter.convert((Document) unionValue.getValue());
            case ENTITY -> EntityToJsonConverter.convert((Entity) unionValue.getValue());
            case MUTATION -> MutationToJsonConverter.convertJsonString((Mutation) unionValue.getValue());
            case MUTATIONGROUP -> MutationToJsonConverter.convertJsonString((MutationGroup) unionValue.getValue());
            case UNIFIEDMUTATION -> UnifiedMutation.toJson((UnifiedMutation) unionValue.getValue());
            default -> throw new IllegalArgumentException("Not supported json conversion: " + unionValue.getType());
        };
    }

    public static Row getAsRow(final Schema schema, final UnionValue unionValue) {
        if(unionValue == null) {
            return null;
        }
        return switch (unionValue.getType()) {
            case ROW -> (Row) unionValue.getValue();
            case AVRO -> AvroToRowConverter.convert(schema, (GenericRecord) unionValue.getValue());
            case STRUCT -> StructToRowConverter.convert(schema, (Struct) unionValue.getValue());
            case DOCUMENT -> DocumentToRowConverter.convert(schema, (Document) unionValue.getValue());
            case ENTITY -> EntityToRowConverter.convert(schema, (Entity) unionValue.getValue());
            case MUTATION -> MutationToRowConverter.convert(schema, (Mutation) unionValue.getValue());
            case UNIFIEDMUTATION -> UnifiedMutation.toRow(schema, (UnifiedMutation) unionValue.getValue());
            default -> throw new IllegalArgumentException();
        };
    }

    public static GenericRecord getAsRecord(final org.apache.avro.Schema schema, final UnionValue unionValue) {
        if(unionValue == null) {
            return null;
        }
        return switch (unionValue.getType()) {
            case ROW -> RowToRecordConverter.convert(schema, (Row) unionValue.getValue());
            case AVRO -> (GenericRecord) unionValue.getValue();
            case STRUCT -> StructToAvroConverter.convert(schema, (Struct) unionValue.getValue());
            case DOCUMENT -> DocumentToAvroConverter.convert(schema, (Document) unionValue.getValue());
            case ENTITY -> EntityToAvroConverter.convert(schema, (Entity) unionValue.getValue());
            case MUTATION -> MutationToAvroConverter.convert(schema, (Mutation) unionValue.getValue());
            case UNIFIEDMUTATION -> UnifiedMutation.toGenericRecord(schema, (UnifiedMutation) unionValue.getValue());
            default -> throw new IllegalArgumentException();
        };
    }

    public static DynamicMessage getAsProtoMessage(final Descriptors.Descriptor messageDescriptor, final UnionValue unionValue) {
        if(unionValue == null) {
            return null;
        }
        return switch (unionValue.getType()) {
            case ROW -> RowToProtoConverter.convert(messageDescriptor, (Row) unionValue.getValue());
            case AVRO -> AvroToProtoConverter.convert(messageDescriptor, (GenericRecord) unionValue.getValue());
            case STRUCT -> StructToProtoConverter.convert(messageDescriptor, (Struct) unionValue.getValue());
            case ENTITY -> EntityToProtoConverter.convert(messageDescriptor, (Entity) unionValue.getValue());
            default -> throw new IllegalArgumentException();
        };
    }

    public static <InputSchemaT, RuntimeSchemaT> RuntimeSchemaT convertSchema(final InputSchemaT inputSchema, final DataType dataType) {
        return switch (dataType) {
            case AVRO -> (RuntimeSchemaT) AvroSchemaUtil.convertSchema((String) inputSchema);
            case ROW, STRUCT, DOCUMENT, ENTITY -> (RuntimeSchemaT) inputSchema;
            default -> throw new IllegalArgumentException("Not supported schema convert: " + dataType);
        };
    }

    public static <RuntimeSchemaT> RuntimeSchemaT convertRowSchema(final Schema inputSchema, final DataType dataType) {
        return switch (dataType) {
            case AVRO -> (RuntimeSchemaT) RowToRecordConverter.convertSchema(inputSchema);
            case ROW, DOCUMENT, ENTITY -> (RuntimeSchemaT) inputSchema;
            case STRUCT -> (RuntimeSchemaT) RowToMutationConverter.convertSchema(inputSchema);
            default -> throw new IllegalArgumentException("Not supported schema convert: " + dataType);
        };
    }

    public static <SchemaT, ElementT> ElementT convert(final SchemaT schema, final UnionValue unionValue, final DataType outputType) {
        if(unionValue == null) {
            return null;
        }

        if(unionValue.getType().equals(outputType)) {
            return (ElementT) unionValue.getValue();
        }

        return switch (outputType) {
            case AVRO -> (ElementT) getAsRecord((org.apache.avro.Schema) schema, unionValue);
            case ROW -> (ElementT) getAsRow((Schema) schema, unionValue);
            default -> throw new IllegalArgumentException("Not supported convert data type: " + outputType);
        };
    }

    public static <SchemaT, ElementT> ElementT create(final SchemaT schema, final Map<String, Object> values, final DataType dataType) {
        return switch (dataType) {
            case AVRO -> (ElementT) AvroSchemaUtil.create((org.apache.avro.Schema) schema, values);
            case ROW -> (ElementT) RowSchemaUtil.create((Schema) schema, values);
            case STRUCT -> (ElementT) StructSchemaUtil.create((Type) schema, values);
            case DOCUMENT -> (ElementT) DocumentSchemaUtil.create((Schema) schema, values);
            case ENTITY -> (ElementT) EntitySchemaUtil.create((Schema) schema, values);
            default -> throw new IllegalArgumentException("Not supported create data type: " + dataType);
        };
    }

    public static Object merge(final UnionValue unionValue, Object schema, Map<String, Object> updates, DataType dataType) {
        if(unionValue.value == null) {
            return null;
        }

        switch (unionValue.type) {
            case ROW -> {
                final Row row = (Row) unionValue.value;
                switch (dataType) {
                    case ROW -> {
                        return RowSchemaUtil.merge((Schema) schema, row, updates);
                    }
                    case AVRO -> {
                        final org.apache.avro.Schema avroSchema = (org.apache.avro.Schema) schema;
                        final GenericRecord record = RowToRecordConverter.convert(avroSchema, row);
                        return AvroSchemaUtil.merge(avroSchema, record, updates);
                    }
                    default ->
                            throw new IllegalStateException("Not supported conversion. from beam row to " + dataType.name());
                }
            }
            case AVRO -> {
                final GenericRecord record = (GenericRecord) unionValue.value;
                switch (dataType) {
                    case ROW -> {
                        final Schema rowSchema = (Schema) schema;
                        final Row row = AvroToRowConverter.convert(rowSchema, record);
                        return RowSchemaUtil.merge(rowSchema, row, updates);
                    }
                    case AVRO -> {
                        return AvroSchemaUtil.merge((org.apache.avro.Schema) schema, record, updates);
                    }
                    default ->
                            throw new IllegalStateException("Not supported conversion. from avro record to " + dataType.name());
                }
            }
            case STRUCT -> {
                final Struct struct = (Struct) unionValue.value;
                switch (dataType) {
                    case ROW -> {
                        final Schema rowSchema = (Schema) schema;
                        final Row row = StructToRowConverter.convert(rowSchema, struct);
                        return RowSchemaUtil.merge(rowSchema, row, updates);
                    }
                    case AVRO -> {
                        final org.apache.avro.Schema avroSchema = (org.apache.avro.Schema) schema;
                        final GenericRecord record = StructToAvroConverter.convert(avroSchema, struct);
                        return AvroSchemaUtil.merge(avroSchema, record, updates);
                    }
                    default ->
                            throw new IllegalStateException("Not supported conversion. from spanner struct to " + dataType.name());
                }
            }
            case DOCUMENT -> {
                final Document document = (Document) unionValue.value;
                switch (dataType) {
                    case ROW -> {
                        final Schema rowSchema = (Schema) schema;
                        final Row row = DocumentToRowConverter.convert(rowSchema, document);
                        return RowSchemaUtil.merge(rowSchema, row, updates);
                    }
                    case AVRO -> {
                        final org.apache.avro.Schema avroSchema = (org.apache.avro.Schema) schema;
                        final GenericRecord record = DocumentToAvroConverter.convert(avroSchema, document);
                        return AvroSchemaUtil.merge(avroSchema, record, updates);
                    }
                    default ->
                            throw new IllegalStateException("Not supported conversion. from firestore document to " + dataType.name());
                }
            }
            case ENTITY -> {
                final Entity entity = (Entity) unionValue.value;
                switch (dataType) {
                    case ROW -> {
                        final Schema rowSchema = (Schema) schema;
                        final Row row = EntityToRowConverter.convert(rowSchema, entity);
                        return RowSchemaUtil.merge(rowSchema, row, updates);
                    }
                    case AVRO -> {
                        final org.apache.avro.Schema avroSchema = (org.apache.avro.Schema) schema;
                        final GenericRecord record = EntityToAvroConverter.convert(avroSchema, entity);
                        return AvroSchemaUtil.merge(avroSchema, record, updates);
                    }
                    default ->
                            throw new IllegalStateException("Not supported conversion. from datastore entity to " + dataType.name());
                }
            }
            default ->
                    throw new IllegalStateException("Union.merge not supported data type: " + unionValue.type.name());
        }

    }

    public static <SchemaT, T> Coder<T> createCoder(final SchemaT schema, final DataType outputType) {
        return switch (outputType) {
            case ROW -> (Coder<T>) RowCoder.of((Schema) schema);
            case AVRO -> (Coder<T>) AvroGenericCoder.of((org.apache.avro.Schema) schema);
            case STRUCT -> (Coder<T>) SerializableCoder.of(Struct.class);
            case DOCUMENT -> (Coder<T>) SerializableCoder.of(Document.class);
            case ENTITY -> (Coder<T>) SerializableCoder.of(Entity.class);
            default -> throw new IllegalArgumentException();
        };
    }

    public static <T> Coder<T> createCoderWithRowSchema(final Schema schema, final DataType outputType) {
        return switch (outputType) {
            case ROW -> (Coder<T>) RowCoder.of(schema);
            case AVRO -> (Coder<T>) AvroGenericCoder.of(RowToRecordConverter.convertSchema(schema));
            case STRUCT -> (Coder<T>) SerializableCoder.of(Struct.class);
            case DOCUMENT -> (Coder<T>) SerializableCoder.of(Document.class);
            case ENTITY -> (Coder<T>) SerializableCoder.of(Entity.class);
            default -> throw new IllegalArgumentException();
        };
    }

    @Override
    public String toString() {
        if(this.value == null) {
            return "null";
        }
        return this.value.toString();
    }

}
