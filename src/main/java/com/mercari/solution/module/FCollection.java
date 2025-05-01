package com.mercari.solution.module;

import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.spanner.Type;
import com.mercari.solution.util.schema.converter.*;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;

import java.util.HashMap;
import java.util.Map;


public class FCollection<T> {

    private final String name;
    private final Map<String, String> properties;

    private final PCollection<T> collection;
    private final DataType dataType;
    private final Schema schema;
    private final org.apache.avro.Schema avroSchema;
    private final Type spannerType;

    // for support multiple collection
    private final Boolean isTuple;
    private final PCollectionTuple tuple;
    private final Map<TupleTag<?>, DataType> dataTypes;
    private final Map<TupleTag<?>, org.apache.avro.Schema> avroSchemas;


    private FCollection(
            final String name,
            final Map<String, String> properties,
            final PCollection<T> pCollection,
            final DataType dataType,
            final Schema schema,
            final org.apache.avro.Schema avroSchema,
            final Type spannerType) {

        this.name = name;
        this.properties = properties;

        this.collection = pCollection;
        this.dataType = dataType;
        this.schema = schema;
        this.avroSchema = avroSchema;
        this.spannerType = spannerType;

        this.isTuple = false;
        this.tuple = null;
        this.dataTypes = new HashMap<>();
        this.avroSchemas = new HashMap<>();
    }

    private FCollection(
            final String name,
            final Map<String, String> properties,
            final PCollectionTuple tuple,
            final Map<TupleTag<?>, DataType> dataTypes,
            final Map<TupleTag<?>, org.apache.avro.Schema> avroSchemas) {

        this.name = name;
        this.properties = properties;

        this.collection = null;
        this.dataType = null;
        this.schema = null;
        this.avroSchema = null;
        this.spannerType = null;

        this.isTuple = true;
        this.tuple = tuple;
        this.dataTypes = dataTypes;
        this.avroSchemas = avroSchemas;
    }

    public static <T> FCollection<T> of(
            final String name,
            final PCollection<T> pCollection,
            final DataType dataType,
            final Schema schema) {

        final Map<String, String> properties = updateProperties(name, null);
        return new FCollection<>(name, properties, pCollection, dataType, schema, null, null);
    }

    public static <T> FCollection<T> of(
            final String name,
            final PCollection<T> pCollection,
            final DataType dataType,
            final org.apache.avro.Schema avroSchema) {

        final Map<String, String> properties = updateProperties(name, null);
        return new FCollection<>(name, properties, pCollection, dataType, null, avroSchema, null);
    }

    public static <T> FCollection<T> of(
            final String name,
            final PCollection<T> pCollection,
            final DataType dataType,
            final Type spannerType) {

        final Map<String, String> properties = updateProperties(name, null);
        return new FCollection<>(name, properties, pCollection, dataType, null, null, spannerType);
    }

    public static <T> FCollection<T> of(
            final String name,
            final Map<String, String> props,
            final PCollectionTuple tuple,
            final Map<TupleTag<?>, DataType> dataTypes,
            final Map<TupleTag<?>, org.apache.avro.Schema> avroSchemas) {

        final Map<String, String> properties = updateProperties(name, props);
        return new FCollection<>(name, properties, tuple, dataTypes, avroSchemas);
    }

    public static <T> FCollection<T> update(final FCollection<T> base, final PCollection<T> pCollection) {
        return update(base, base.getName(), pCollection);
    }

    public static <T> FCollection<T> update(final FCollection<T> base, final String name, final PCollection<T> pCollection) {
        return new FCollection<>(name, base.getProperties(), pCollection, base.getDataType(), base.getSchema(), base.getAvroSchema(), base.getSpannerType());
    }

    public String getName() {
        return name;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public PCollection<T> getCollection() {
        return collection;
    }

    public PCollectionTuple getTuple() {
        return tuple;
    }

    public DataType getDataType() {
        return dataType;
    }

    public Schema getSchema() {
        if(this.schema != null) {
            return schema;
        } else if(this.avroSchema != null) {
            return AvroToRowConverter.convertSchema(avroSchema);
        } else if(this.spannerType != null) {
            return StructToRowConverter.convertSchema(spannerType);
        } else {
            if(DataType.MUTATIONGROUP.equals(dataType) || DataType.MUTATION.equals(dataType)) {
                return null;
            }
            throw new IllegalArgumentException("FCollection has no schemas!");
        }
    }

    public org.apache.avro.Schema getAvroSchema() {
        if(this.avroSchema != null) {
            return avroSchema;
        } else if(this.schema != null) {
            return RowToRecordConverter.convertSchema(schema);
        } else if(this.spannerType != null) {
            return StructToAvroConverter.convertSchema(spannerType);
        } else {
            switch (dataType) {
                case MUTATION:
                case MUTATIONGROUP:
                    return null;
            }
            throw new IllegalArgumentException("FCollection has no schemas!");
        }
    }

    public Type getSpannerType() {
        if(this.spannerType != null) {
            return spannerType;
        } else if(this.avroSchema != null) {
            return AvroToMutationConverter.convertSchema(this.avroSchema);
        } else if(this.schema != null) {
            return RowToMutationConverter.convertSchema(this.schema);
        } else {
            switch (dataType) {
                case MUTATION:
                case MUTATIONGROUP:
                    return null;
            }
            throw new IllegalArgumentException("FCollection has no SpannerType!");
        }
    }

    public TableSchema getTableSchema() {
        if(this.schema != null) {
            return RowToTableRowConverter.convertTableSchema(schema);
        } else if(this.avroSchema != null) {
            return AvroToTableRowConverter.convertSchema(avroSchema);
        } else if(this.spannerType != null) {
            return StructToTableRowConverter.convertSchema(spannerType);
        } else {
            switch (dataType) {
                case MUTATION:
                case MUTATIONGROUP:
                    return null;
            }
            throw new IllegalArgumentException("FCollection has no schemas!");
        }
    }

    public Boolean getIsTuple() {
        return isTuple;
    }

    public Map<TupleTag<?>, DataType> getDataTypes() {
        return dataTypes;
    }

    public Map<TupleTag<?>, org.apache.avro.Schema> getAvroSchemas() {
        return avroSchemas;
    }

    private static Map<String, String> updateProperties(final String name, final Map<String, String> props) {
        final Map<String, String> properties = new HashMap<>();
        if(props == null) {
            return properties;
        }
        for(final Map.Entry<String, String> entry : props.entrySet()) {
            if(entry.getKey() == null) {
                continue;
            }
            final String key;
            if(entry.getKey().startsWith(name + ".")) {
                key = entry.getKey();
            } else {
                key = name + "." + entry.getKey();
            }
            properties.put(key, entry.getValue());
        }
        return properties;
    }

}
