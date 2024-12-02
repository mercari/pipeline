package com.mercari.solution.module;

import com.google.api.services.bigquery.model.TableSchema;
import com.google.gson.*;
import com.google.protobuf.Descriptors;
import com.mercari.solution.util.DateTimeUtil;
import com.mercari.solution.util.gcp.StorageUtil;
import com.mercari.solution.util.schema.AvroSchemaUtil;
import com.mercari.solution.util.schema.ProtoSchemaUtil;
import com.mercari.solution.util.schema.converter.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

public class Schema implements Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(Schema.class);

    public static final String OPTION_DEFAULT_VALUE = "default";
    public static final String OPTION_ORIGINAL_FIELD_NAME = "originalFieldName";

    private String name;
    private DataType type;

    private List<Field> fields;
    private Map<String, String> options;

    // extension schemas
    private RowSchema row;
    private AvroSchema avro;
    private ProtobufSchema protobuf;

    private Boolean useDestinationSchema;

    public String getName() {
        return name;
    }

    public DataType getType() {
        return type;
    }

    public List<Field> getFields() {
        if(this.fields == null || this.fields.isEmpty()) {
            this.setup();
        }
        return fields;
    }

    public boolean hasField(final String name) {
        return fields.stream()
                .anyMatch(f -> f.getName().equals(name));
    }

    public Field getField(int index) {
        return fields.get(index);
    }

    public Field getField(final String name) {
        return fields.stream()
                .filter(f -> f.getName().equals(name))
                .findAny()
                .orElse(null);
    }

    public boolean hasOption(final String name) {
        if(options == null) {
            return false;
        }
        return options.containsKey(name);
    }

    public RowSchema getRow() {
        if(row == null && fields != null && !fields.isEmpty()) {
            row = new RowSchema(ElementToRowConverter.convertSchema(fields));
        }
        return row;
    }

    public AvroSchema getAvro() {
        if(avro == null && fields != null && !fields.isEmpty()) {
            avro = new AvroSchema(ElementToAvroConverter.convertSchema(fields));
        }
        return avro;
    }

    public ProtobufSchema getProtobuf() {
        return protobuf;
    }

    public org.apache.beam.sdk.schemas.Schema getRowSchema() {
        return Optional
                .ofNullable(getRow())
                .map(RowSchema::getSchema)
                .orElseGet(() -> ElementToRowConverter.convertSchema(fields));
    }

    public org.apache.avro.Schema getAvroSchema() {
        return Optional
                .ofNullable(getAvro())
                .map(AvroSchema::getSchema)
                .orElseGet(() -> ElementToAvroConverter.convertSchema(fields));
    }

    public Boolean getUseDestinationSchema() {
        return useDestinationSchema;
    }

    public int countFields() {
        return switch (type) {
            case ELEMENT, DOCUMENT, ENTITY -> this.fields.size();
            case ROW -> this.row.schema.getFieldCount();
            case AVRO -> this.avro.schema.getFields().size();
            default -> throw new IllegalArgumentException();
        };
    }

    public Schema withName(String name) {
        this.name = name;
        return this;
    }

    public Schema withType(DataType type) {
        this.type = type;
        return this;
    }

    public void setup() {
        setup(DataType.ELEMENT);
    }

    public void setup(final DataType dataType) {
        switch (dataType) {
            case AVRO -> {
                if(avro == null) {
                    if(fields != null && !fields.isEmpty()) {
                        avro = new AvroSchema(ElementToAvroConverter.convertSchema(this.fields));
                    }
                }
                avro.setup();
            }
            case ROW -> {
                if(row == null) {
                    if(fields != null && !fields.isEmpty()) {
                        row = new RowSchema(ElementToRowConverter.convertSchema(fields));
                    } else if(avro != null) {
                        row = new RowSchema(AvroToRowConverter.convertSchema(avro.schema));
                    }
                }
            }
        }
        if(protobuf != null) {
            protobuf.setup();
        }
        if(fields == null || fields.isEmpty()) {
            if(avro != null) {
                this.fields = AvroToElementConverter.convertFields(avro.schema.getFields());
            } else if(protobuf != null) {
                this.fields = ProtoToElementConverter.convertFields(protobuf.descriptors.get(protobuf.messageName));
            } else if(row != null) {
                this.fields = RowToElementConverter.convertFields(row.schema.getFields());
            }
        }

    }

    private Schema(
            DataType type,
            List<Field> fields,
            org.apache.beam.sdk.schemas.Schema rowSchema,
            org.apache.avro.Schema avroSchema) {

        this.type = type;
        this.fields = fields;
        this.row = new RowSchema(rowSchema);
        this.avro = new AvroSchema(avroSchema);
        this.protobuf = new ProtobufSchema();
    }

    public static Schema parse(final String jsonText) {
        return parse(new Gson().fromJson(jsonText, JsonObject.class));
    }

    public static Schema parse(final JsonElement jsonElement) {
        if(jsonElement == null || jsonElement.isJsonNull() || !jsonElement.isJsonObject()) {
            return null;
        }
        return parse(jsonElement.getAsJsonObject());
    }

    public static Schema parse(final JsonObject jsonObject) {
        if(jsonObject == null || jsonObject.isJsonNull()) {
            return null;
        }

        final Schema.Builder builder = Schema.builder();
        final List<String> errorMessages = new ArrayList<>();

        if(jsonObject.has("fields")) {
            final JsonElement fieldsElement = jsonObject.get("fields");
            if(!fieldsElement.isJsonArray()) {
                errorMessages.add("schema.fields must be array. but: " + fieldsElement);
            } else {
                final List<Field> fields = new ArrayList<>();
                int i=0;
                for(final JsonElement fieldElement : fieldsElement.getAsJsonArray()) {
                    if(!fieldElement.isJsonObject()) {
                        throw new IllegalArgumentException("schema.fields[" + i + "] must be object. but: " + fieldsElement);
                    }
                    final Field field = Field.parse(fieldElement.getAsJsonObject());
                    if(field == null) {
                        errorMessages.add("schema.fields[" + i + "] is illegal format: " + fieldElement);
                    } else {
                        fields.add(field);
                    }
                    i++;
                }
                builder.withFields(fields);
            }
        }

        if(jsonObject.has("avro")) {
            final JsonElement avroElement = jsonObject.get("avro");
            if(!avroElement.isJsonObject()) {
                errorMessages.add("schema.avro must be object. but: " + avroElement);
            } else {
                final AvroSchema avro = AvroSchema.parse(avroElement.getAsJsonObject());
                if(avro == null) {
                    errorMessages.add("schema.avro is illegal format: " + avroElement);
                } else {
                    builder.withAvro(avro);
                }
            }
        }

        if(jsonObject.has("protobuf")) {
            final JsonElement avroElement = jsonObject.get("protobuf");
            if(!avroElement.isJsonObject()) {
                errorMessages.add("schema.protobuf must be object. but: " + avroElement);
            } else {
                final ProtobufSchema protobuf = ProtobufSchema.parse(avroElement.getAsJsonObject());
                if(protobuf == null) {
                    errorMessages.add("schema.protobuf is illegal format: " + avroElement);
                } else {
                    builder.withProtobuf(protobuf);
                }
            }
        }

        if(jsonObject.has("useDestinationSchema")) {
            final JsonElement useDestinationSchemaElement = jsonObject.get("useDestinationSchema");
            if(useDestinationSchemaElement.isJsonPrimitive()) {
                final JsonPrimitive useDestinationSchemaPrimitive = useDestinationSchemaElement.getAsJsonPrimitive();
                if(useDestinationSchemaPrimitive.isBoolean()) {
                    builder.withUseDestinationSchema(useDestinationSchemaPrimitive.getAsBoolean());
                }
            }
        }

        // deprecated
        if(jsonObject.has("avroSchema")) {
            final JsonElement avroElement = jsonObject.get("avroSchema");
            if(!avroElement.isJsonPrimitive()) {
                errorMessages.add("schema.avroSchema must be string. but: " + avroElement);
            }
            final AvroSchema avro = new AvroSchema();
            avro.file = avroElement.getAsString();
            builder.withAvro(avro);
        }
        // deprecated
        if(jsonObject.has("protobufDescriptor")) {
            final JsonElement protobufDescriptorElement = jsonObject.get("protobufDescriptor");
            if(!protobufDescriptorElement.isJsonPrimitive()) {
                errorMessages.add("schema.protobufDescriptor must be string. but: " + protobufDescriptorElement);
            }
            final ProtobufSchema protobuf = new ProtobufSchema();
            protobuf.descriptorFile = protobufDescriptorElement.getAsString();
            builder.withProtobuf(protobuf);
        }

        if (!errorMessages.isEmpty()) {
            throw new IllegalArgumentException(String.join(", ", errorMessages));
        }

        return builder.build();
    }

    public Schema copy() {
        return Schema.builder(this).build();
    }

    public JsonObject toJsonObject() {
        final JsonObject jsonObject = new JsonObject();
        {
            final JsonArray fieldsArray = new JsonArray();
            for(final Field field : fields) {
                fieldsArray.add(field.toJsonObject());
            }
            jsonObject.add("fields", fieldsArray);
        }
        return jsonObject;
    }

    public static Schema of(final List<Field> fields) {
        if(fields.isEmpty()) {
            throw new IllegalArgumentException("schema fields must not be empty");
        }
        return new Schema(DataType.ELEMENT, fields, null, null);
    }

    public static Schema of(org.apache.beam.sdk.schemas.Schema rowSchema) {
        final List<Field> fields = RowToElementConverter.convertFields(rowSchema.getFields());
        return new Schema(DataType.ROW, fields, rowSchema, null);
    }

    public static Schema of(org.apache.avro.Schema avroSchema) {
        final List<Field> fields = AvroToElementConverter.convertFields(avroSchema.getFields());
        return new Schema(DataType.AVRO, fields, null, avroSchema);
    }

    public static Schema of(com.google.cloud.spanner.Type type) {
        final List<Field> fields = StructToElementConverter.convertFields(type.getStructFields());
        return new Schema(DataType.STRUCT, fields, null, null);
    }

    public static Schema of(Descriptors.Descriptor descriptor) {
        return ProtoToElementConverter.convertSchema(descriptor);
    }

    public static Schema of(TableSchema tableSchema) {
        return TableRowToElementConverter.convertSchema(tableSchema);
    }

    public static Schema of(
            final List<Field> fields,
            final AvroSchema avroSchema) {

        if(!fields.isEmpty()) {
            return of(fields);
        }

        if(avroSchema != null) {
            return of(avroSchema.schema);
        }

        throw new IllegalArgumentException();
    }

    public static Builder builder() {
        return new Builder();
    }

    public static Builder builder(Schema schema) {
        return new Builder(schema);
    }

    public static class Field implements Serializable {

        private String name;
        private FieldType type;

        private Map<String, String> options;
        private String description;

        private String alterName;

        public String getName() {
            return name;
        }

        public FieldType getFieldType() {
            return type;
        }

        public Map<String, String> getOptions() {
            return options;
        }

        public String getDescription() {
            return description;
        }

        public String getAlterName() {
            return alterName;
        }

        public void setAlterName(String alterName) {
            this.alterName = alterName;
        }

        public Field withOptions(Map<String, String> options) {
            this.options = options;
            return this;
        }

        public Field withDescription(String description) {
            this.description = description;
            return this;
        }

        public Field copy() {
            final Field field = new Field();
            field.name = this.name;
            field.type = this.type != null ? this.type.copy() : null;
            field.options = this.options != null ? new HashMap<>(this.options) : new HashMap<>();
            field.description = this.description;
            field.alterName = this.alterName;
            return field;
        }

        Field() {
            this.options = new HashMap<>();
        }

        Field(String name, FieldType type) {
            this.name = name;
            this.type = type;
            this.options = new HashMap<>();
        }

        public static Field parse(final JsonObject jsonObject) {
            if(!jsonObject.has("name") || !jsonObject.has("type")) {
                return null;
            }

            final String name = jsonObject.get("name").getAsString();
            final String typeString = jsonObject.get("type").getAsString();
            final String modeString = jsonObject.has("mode") ? jsonObject.get("mode").getAsString() : null;
            final Type type = Type.of(typeString);
            final Mode mode = Mode.of(modeString);

            final FieldType fieldType = FieldType.parse(name, type, mode, jsonObject);
            return Field.of(name, fieldType);
        }

        public static Field of(final String name, final FieldType type) {
            return new Field(name, type);
        }

        @Override
        public String toString() {
            return String.format("name: " + name + ", type: " + type);
        }

        public JsonObject toJsonObject() {
            final JsonObject jsonObject = new JsonObject();
            jsonObject.addProperty("name", name);
            jsonObject.add("type", type.toJsonObject());
            return jsonObject;
        }

    }

    public static class FieldType implements Serializable {

        public static FieldType BOOLEAN = FieldType.type(Type.bool);
        public static FieldType STRING = FieldType.type(Type.string);
        public static FieldType JSON = FieldType.type(Type.json);
        public static FieldType BYTES = FieldType.type(Type.bytes);
        public static FieldType INT32 = FieldType.type(Type.int32);
        public static FieldType INT64 = FieldType.type(Type.int64);
        public static FieldType FLOAT32 = FieldType.type(Type.float32);
        public static FieldType FLOAT64 = FieldType.type(Type.float64);
        public static FieldType DECIMAL = FieldType.decimal(38, 9);
        public static FieldType BIGDECIMAL = FieldType.decimal(76, 38);
        public static FieldType TIME = FieldType.type(Type.time);
        public static FieldType DATE = FieldType.type(Type.date);
        public static FieldType TIMESTAMP = FieldType.type(Type.timestamp);

        private Type type;
        private Schema elementSchema; // for element type
        private FieldType arrayValueType;
        private FieldType mapValueType; // for map type
        private List<String> symbols; // for enumerate type
        private Integer scale; // for decimal
        private Integer precision; // for decimal
        private Boolean nullable;
        private String defaultValue;

        public Type getType() {
            return type;
        }

        public Schema getElementSchema() {
            return elementSchema;
        }

        public FieldType getArrayValueType() {
            return arrayValueType;
        }

        public FieldType getMapValueType() {
            return mapValueType;
        }

        public List<String> getSymbols() {
            return symbols;
        }

        public Integer getScale() {
            return scale;
        }

        public Integer getPrecision() {
            return precision;
        }

        public Boolean getNullable() {
            return nullable;
        }

        public String getDefaultValue() {
            return defaultValue;
        }

        public Integer getSymbolIndex(String symbol) {
            if(symbols == null || symbol == null) {
                return null;
            }
            for(int index=0; index<symbols.size(); index++) {
                if(symbol.equals(symbols.get(index))) {
                    return index;
                }
            }
            return null;
        }

        private FieldType() {
            this.symbols = new ArrayList<>();
            this.nullable = true;
        }

        public static FieldType of(
                final String name,
                final String typeString,
                final String modeString,
                final List<? extends IField> fields,
                final List<String> symbols,
                final String valueType) {

            final Type type = Type.of(typeString);
            final Mode mode = Mode.of(modeString);

            final JsonObject jsonObject = new JsonObject();
            if(fields != null) {
                final JsonArray fieldsArray = new JsonArray();
                for(final IField f : fields) {
                    final JsonObject fieldJsonObject = IField.toJson(f);
                    fieldsArray.add(fieldJsonObject);
                }
                jsonObject.add("fields", fieldsArray);
            }
            if(symbols != null) {
                final JsonArray symbolsArray = new JsonArray();
                symbols.forEach(symbolsArray::add);
                jsonObject.add("symbols", symbolsArray);
            }
            jsonObject.addProperty("valueType", valueType);

            return parse(name, type, mode, jsonObject);
        }

        public static FieldType parse(
                final String name,
                final Type type,
                final Mode mode,
                final JsonObject jsonObject) {

            final FieldType fieldType;
            if(Type.isPrimitiveType(type)) {
                fieldType = FieldType.type(type);
            } else {
                switch (type) {
                    case element -> {
                        if(!jsonObject.has("fields")) {
                            throw new IllegalArgumentException("field: " + name + " requires 'fields' parameter for record type");
                        } else if(!jsonObject.get("fields").isJsonArray()) {
                            throw new IllegalArgumentException("field: " + name + ".'fields' parameter must be json array. actual value is: " + jsonObject.get("fields"));
                        } else {
                            int i=0;
                            final List<Field> fields = new ArrayList<>();
                            for(final JsonElement fieldElement : jsonObject.getAsJsonArray("fields")) {
                                if(!fieldElement.isJsonObject()) {
                                    throw new IllegalArgumentException("field: " + name + ".fields[" + i + "] must be object. actual value is: " + fieldElement);
                                }
                                final Field field = Field.parse(fieldElement.getAsJsonObject());
                                if(field == null) {
                                    throw new IllegalArgumentException("field: " + name + ".fields[" + i + "] is illegal format: " + fieldElement);
                                } else {
                                    fields.add(field);
                                }
                                i++;
                            }
                            fieldType = FieldType.element(fields);
                        }
                    }
                    case enumeration -> {
                        if(!jsonObject.has("symbols") || !jsonObject.get("symbols").isJsonArray()) {
                            throw new IllegalArgumentException("field: " + name + " requires 'symbols' parameter for enum type");
                        } else {
                            final List<String> symbols = new ArrayList<>();
                            for(final JsonElement symbolElement : jsonObject.getAsJsonArray("symbols")) {
                                symbols.add(symbolElement.getAsString());
                            }
                            fieldType = FieldType.enumeration(symbols);
                        }
                    }
                    case map -> {
                        if(!jsonObject.has("valueType")) {
                            throw new IllegalArgumentException("field: " + name + " requires 'valueType' parameter for map type");
                        } else {
                            final Type valueType = Type.of(jsonObject.get("valueType").getAsString());
                            final FieldType valueFieldType = parse(name, valueType, mode, jsonObject);
                            fieldType = FieldType.map(valueFieldType);
                        }
                    }
                    case array -> throw new IllegalArgumentException("use mode=repeated instead of type=array");
                    default -> throw new IllegalArgumentException();
                }
            }

            final String defaultValue = jsonObject.has("default") ? jsonObject.get("default").getAsString() : null;

            return switch (mode) {
                case repeated -> FieldType.array(fieldType).withDefaultValue(defaultValue);
                case required -> fieldType.withNullable(false).withDefaultValue(defaultValue);
                case nullable -> fieldType.withNullable(true);
            };
        }

        public static FieldType type(String type) {
            return type(Type.of(type));
        }

        public static FieldType type(Type type) {
            switch (type) {
                case element, array, map, enumeration, decimal -> throw new IllegalArgumentException(
                        "type: " + type + " requires more parameters");
            }
            final FieldType fieldType = new FieldType();
            fieldType.type = type;
            fieldType.nullable = true;
            return fieldType;
        }

        public static FieldType element(List<Field> fields) {
            return element(Schema.of(fields));
        }

        public static FieldType element(Schema schema) {
            final FieldType fieldType = new FieldType();
            fieldType.type = Type.element;
            fieldType.elementSchema = schema;
            fieldType.nullable = true;
            return fieldType;
        }

        public static FieldType array(FieldType arrayValueType) {
            final FieldType fieldType = new FieldType();
            fieldType.type = Type.array;
            fieldType.arrayValueType = arrayValueType;
            fieldType.nullable = true;
            return fieldType;
        }

        public static FieldType map(FieldType mapValueType) {
            final FieldType fieldType = new FieldType();
            fieldType.type = Type.map;
            fieldType.mapValueType = mapValueType;
            fieldType.nullable = true;
            return fieldType;
        }

        public static FieldType enumeration(List<String> symbols) {
            final FieldType fieldType = new FieldType();
            fieldType.type = Type.enumeration;
            fieldType.symbols = symbols;
            fieldType.nullable = true;
            return fieldType;
        }

        public static FieldType decimal(int precision, int scale) {
            final FieldType fieldType = new FieldType();
            fieldType.type = Type.decimal;
            fieldType.precision = precision;
            fieldType.scale = scale;
            fieldType.nullable = true;
            return fieldType;
        }

        public FieldType withNullable(final boolean nullable) {
            final FieldType fieldType = this.copy();
            fieldType.nullable = nullable;
            return fieldType;
        }

        public FieldType withDefaultValue(final Object value) {
            final FieldType fieldType = this.copy();
            if(value == null) {
                fieldType.defaultValue = null;
            } else {
                fieldType.defaultValue = value.toString();
            }
            return fieldType;
        }

        public FieldType copy() {
            final FieldType fieldType = new FieldType();
            fieldType.type = this.type;
            fieldType.elementSchema = Optional.ofNullable(this.elementSchema).map(Schema::copy).orElse(null);
            fieldType.arrayValueType = Optional.ofNullable(this.arrayValueType).map(FieldType::copy).orElse(null);
            fieldType.mapValueType = Optional.ofNullable(this.mapValueType).map(FieldType::copy).orElse(null);
            fieldType.symbols = new ArrayList<>(this.symbols);
            fieldType.precision = this.precision;
            fieldType.scale = this.scale;
            fieldType.nullable = this.nullable;
            fieldType.defaultValue = this.defaultValue;
            return fieldType;
        }

        @Override
        public String toString() {
            return switch (type) {
                case element -> String.format("{ type: %s, schema: %s }", type, elementSchema);
                case array -> String.format("{ type: %s, arrayValue: %s }", type, arrayValueType);
                case map -> String.format("{ type: %s, mapValue: %s }", type, mapValueType);
                case enumeration -> String.format("{ type: %s, symbols: %s }", type, symbols);
                default -> String.format("{ type: %s }", type);
            };
        }

        public JsonObject toJsonObject() {
            final JsonObject jsonObject = new JsonObject();
            jsonObject.addProperty("type", type.name());
            jsonObject.addProperty("nullable", nullable);
            switch (type) {
                case element -> {
                    jsonObject.add("schema", elementSchema.toJsonObject());
                }
                case array -> {
                    jsonObject.add("arrayValueType", arrayValueType.toJsonObject());
                }
                case map -> {
                    jsonObject.add("mapValueType", mapValueType.toJsonObject());
                }
                case enumeration -> {
                    final JsonArray symbolsArray = new JsonArray();
                    if(symbols != null) {
                        for(final String symbol : symbols) {
                            symbolsArray.add(symbol);
                        }
                    }
                    jsonObject.add("enumSymbols", symbolsArray);
                }
            }

            return jsonObject;
        }

    }

    public static class RowSchema implements Serializable {

        private org.apache.beam.sdk.schemas.Schema schema;

        RowSchema() {

        }

        RowSchema(org.apache.beam.sdk.schemas.Schema schema) {
            this.schema = schema;
        }

        public org.apache.beam.sdk.schemas.Schema getSchema() {
            return schema;
        }

        @Override
        public String toString() {
            if(schema == null) {
                return "";
            }
            return schema.toString();
        }

    }

    public static class AvroSchema implements Serializable {

        private String json;
        private String file;

        private transient org.apache.avro.Schema schema;

        AvroSchema() {

        }

        AvroSchema(org.apache.avro.Schema schema) {
            this.schema = schema;
            this.json = Optional.ofNullable(schema).map(s -> s.toString()).orElse(null);
        }

        private void setup() {
            if(this.json == null) {
                if(this.file != null) {
                    this.json = StorageUtil.readString(file);
                }
            }
            if(this.schema == null && this.json != null) {
                this.schema = AvroSchemaUtil.convertSchema(json);
            }
        }

        public String getJson() {
            return json;
        }

        public String getFile() {
            return file;
        }

        public org.apache.avro.Schema getSchema() {
            if(schema == null) {
                if((file != null || json != null)) {
                    setup();
                } else {
                    throw new IllegalArgumentException("Avro schema is null");
                }
            }
            return schema;
        }

        public static AvroSchema parse(final JsonObject jsonObject) {
            return new Gson().fromJson(jsonObject, AvroSchema.class);
        }
    }

    public static class ProtobufSchema implements Serializable {

        private String proto;
        private String descriptorFile;
        private String messageName;

        private transient Map<String, Descriptors.Descriptor> descriptors;

        public String getProto() {
            return proto;
        }

        public String getDescriptorFile() {
            return descriptorFile;
        }

        public String getMessageName() {
            return messageName;
        }

        public Descriptors.Descriptor getDescriptor() {
            return descriptors.get(messageName);
        }

        private void setup() {
            if(this.descriptorFile != null) {
                final byte[] bytes = StorageUtil.readBytes(this.descriptorFile);
                final Map<String, Descriptors.Descriptor> descriptors = ProtoSchemaUtil.getDescriptors(bytes);
                if(messageName != null) {
                    this.descriptors = new HashMap<>();
                    this.descriptors.put(messageName, descriptors.get(messageName));
                    LOG.info("load descriptor: {}, messageName: {}", descriptors.get(messageName), messageName);
                } else {
                    this.descriptors = descriptors;
                    LOG.info("load descriptors: {}", descriptors);
                }
            } else {
                LOG.warn("descriptorFile is null");
            }
        }

        public static ProtobufSchema parse(final JsonObject jsonObject) {
            return new Gson().fromJson(jsonObject, ProtobufSchema.class);
        }
    }


    public static class Builder implements Serializable {

        private String name;
        private DataType type;
        private List<Field> fields;
        private AvroSchema avro;
        private RowSchema row;
        private ProtobufSchema protobuf;
        private Boolean useDestinationSchema;

        private Builder() {
            this("", new ArrayList<>());
        }

        private Builder(String name, List<Field> fields) {
            this.name = name;
            this.fields = fields;
        }

        private Builder(Schema schema) {
            if(schema == null) {
                this.name = "";
                this.fields = new ArrayList<>();
                return;
            }

            this.name = schema.name;
            this.type = schema.type;
            this.fields = schema.fields.stream()
                    .map(Field::copy)
                    .collect(Collectors.toList());
            if(schema.avro != null) {
                this.avro = new AvroSchema();
                this.avro.file = schema.avro.file;
                this.avro.json = schema.avro.json;
                this.avro.schema = schema.avro.schema;
            }
            if(schema.protobuf != null) {
                this.protobuf = new ProtobufSchema();
                this.protobuf.proto = schema.protobuf.proto;
                this.protobuf.descriptorFile = schema.protobuf.descriptorFile;
                this.protobuf.messageName = schema.protobuf.messageName;
                this.protobuf.descriptors = schema.protobuf.descriptors;
            }
            if(schema.row != null) {
                this.row = new RowSchema();
                this.row.schema = schema.row.schema;
            }
        }

        public Schema build() {
            final Schema schema;
            if(fields.isEmpty()) {
                if(protobuf != null) {
                    protobuf.setup();
                    schema = Schema.of(protobuf.descriptors.get(protobuf.messageName));
                    schema.protobuf.descriptorFile = protobuf.descriptorFile;
                    schema.protobuf.messageName = protobuf.messageName;
                } else if(row != null) {
                    schema = Schema.of(row.schema);
                } else if(avro != null) {
                    avro.setup();
                    schema = Schema.of(avro.schema);
                    schema.avro.json = avro.json;
                    schema.avro.file = avro.file;
                } else {
                    throw new IllegalArgumentException();
                }
            } else {
                schema = Schema.of(fields);
                schema.avro = avro;
                schema.protobuf = protobuf;
                schema.row = row;
            }
            schema.name = name;
            schema.useDestinationSchema = useDestinationSchema;
            if(type != null) {
                schema.type = type;
            }
            //schema.setup();
            return schema;
        }

        public Builder withField(String name, FieldType type) {
            final Field field = Field.of(name, type);
            this.fields.add(field);
            return this;
        }

        public Builder withField(Field field) {
            this.fields.add(field);
            return this;
        }

        public Builder withFields(List<Field> fields) {
            this.fields.addAll(fields);
            return this;
        }

        public Builder withAvro(AvroSchema avro) {
            this.avro = avro;
            return this;
        }

        public Builder withProtobuf(ProtobufSchema protobuf) {
            this.protobuf = protobuf;
            return this;
        }

        public Builder withUseDestinationSchema(Boolean useDestinationSchema) {
            this.useDestinationSchema = useDestinationSchema;
            return this;
        }

        public Builder withType(DataType type) {
            this.type = type;
            return this;
        }

    }

    public enum Type {
        bool,
        string,
        json,
        bytes,
        int8,
        int16,
        int32,
        int64,
        float8,
        float16,
        float32,
        float64,
        decimal,
        geography,
        date,
        time,
        datetime,
        timestamp,
        enumeration,
        map,
        element,
        array;

        public static Type of(final String type) {
            if(type == null) {
                throw new IllegalArgumentException("schema type must not be null");
            }
            return switch (type.trim().toLowerCase()) {
                case "bool", "boolean" -> Type.bool;
                case "bytes" -> Type.bytes;
                case "string" -> Type.string;
                case "json" -> Type.json;
                case "byte", "int8" -> Type.int8;
                case "short", "int16" -> Type.int16;
                case "int", "int32", "integer" -> Type.int32;
                case "long", "int64" -> Type.int64;
                case "float16" -> Type.float16;
                case "float", "float32" -> Type.float32;
                case "double", "float64" -> Type.float64;
                case "numeric", "decimal" -> Type.decimal;
                case "geography" -> Type.geography;
                case "time" -> Type.time;
                case "date" -> Type.date;
                case "datetime" -> Type.datetime;
                case "timestamp" -> Type.timestamp;
                case "enum", "enumeration" -> Type.enumeration;
                case "row", "struct", "record", "element" -> Type.element;
                case "map" -> Type.map;
                default -> throw new IllegalArgumentException("Not supported schema type[" + type + "]");
            };
        }

        public static Object getValue(String type, JsonElement value) {
            return switch (type) {
                case "bool", "boolean" -> value.getAsBoolean();
                case "int", "int32", "integer" -> value.getAsInt();
                case "long", "int64" -> value.getAsLong();
                case "float", "float32" -> value.getAsFloat();
                case "double", "float64" -> value.getAsDouble();
                case "string", "json" -> value.getAsString();
                case "date" -> Long.valueOf(DateTimeUtil.toLocalDate(value.getAsString()).toEpochDay()).intValue();
                case "time" -> DateTimeUtil.toLocalTime(value.getAsString()).toNanoOfDay() / 1000L;
                case "timestamp" -> DateTimeUtil.toJodaInstant(value.getAsString()).getMillis() * 1000L;
                default -> throw new IllegalArgumentException("SelectField type[" + type + "] is not supported");
            };
        }

        public static boolean isPrimitiveType(final String typeString) {
            final Type type = of(typeString);
            return isPrimitiveType(type);
        }

        public static boolean isPrimitiveType(final Type type) {
            return switch (type) {
                case element, array, map, enumeration, decimal, geography -> false;
                default -> true;
            };
        }
    }

    public enum Mode {
        nullable,
        required,
        repeated;

        static Mode of(final String mode) {
            if(mode == null) {
                return Mode.nullable;
            }
            return switch (mode.trim().toLowerCase()) {
                case "nullable" -> Mode.nullable;
                case "required" -> Mode.required;
                case "repeated" -> Mode.repeated;
                default -> throw new IllegalArgumentException("Not supported schema mode[" + mode + "]");
            };
        }
    }

    @Override
    public String toString() {
        return String.format("type: " + type + ", fields: " + fields);
    }

    public interface IField extends Serializable {
        String getName();
        String getType();
        String getMode();
        List<? extends IField> getFields();
        List<String> getSymbols();
        String getValueType();

        static Field toField(IField field) {
            final JsonObject jsonObject = toJson(field);
            return Field.parse(jsonObject);
        }

        static FieldType toFieldType(IField field) {
            final JsonObject jsonObject = toJson(field);
            final Field f = Field.parse(jsonObject);
            if(f == null) {
                return null;
            }
            return f.getFieldType();
        }

        static JsonObject toJson(IField field) {
            final JsonObject fieldJsonObject = new JsonObject();
            fieldJsonObject.addProperty("name", field.getName());
            fieldJsonObject.addProperty("type", field.getType());
            if(field.getMode() != null) {
                fieldJsonObject.addProperty("mode", field.getMode());
            }
            if(field.getSymbols() != null) {
                final JsonArray symbolsArray = new JsonArray();
                fieldJsonObject.add("symbols", symbolsArray);
            }
            if(field.getFields() != null) {
                final JsonArray fieldsArray = new JsonArray();
                for(final IField f : field.getFields()) {
                    final JsonObject o = toJson(f);
                    fieldsArray.add(o);
                }
                fieldJsonObject.add("fields", fieldsArray);
            }
            fieldJsonObject.addProperty("valueType", field.getValueType());
            return fieldJsonObject;
        }
    }

}
