package com.mercari.solution.util.pipeline.select;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.mercari.solution.module.Schema;
import com.mercari.solution.util.schema.converter.JsonToMapConverter;
import com.mercari.solution.util.schema.ElementSchemaUtil;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class Struct implements SelectFunction {

    private static final Logger LOG = LoggerFactory.getLogger(Struct.class);

    private final String name;
    private final List<SelectFunction> selectFunctions;

    private final List<Schema.Field> inputFields;
    private final Schema.FieldType outputFieldType;
    private final Schema.Field eachField;
    private final String eachFieldPath;
    private final boolean isArray;
    private final boolean ignore;

    Struct(final String name,
           final List<SelectFunction> selectFunctions,
           final Schema.FieldType outputFieldType,
           final List<Schema.Field> inputFields,
           final Schema.Field eachField,
           final String eachFieldPath,
           final boolean isArray,
           final boolean ignore) {

        this.name = name;
        this.selectFunctions = selectFunctions;
        this.inputFields = inputFields;
        this.outputFieldType = outputFieldType;
        this.eachField = eachField;
        this.eachFieldPath = eachFieldPath;
        this.isArray = isArray;
        this.ignore = ignore;
    }

    public static Struct of(String name, JsonObject jsonObject, List<Schema.Field> inputFields, boolean ignore) {
        if(!jsonObject.has("fields")) {
            throw new IllegalArgumentException("SelectField: " + name + " struct func requires fields parameter");
        } else if(!jsonObject.get("fields").isJsonArray()) {
            throw new IllegalArgumentException("SelectField: " + name + " struct func fields parameter must be array");
        }

        final String eachFieldName;
        if(jsonObject.has("each") && jsonObject.get("each").isJsonPrimitive()) {
            eachFieldName = jsonObject.get("each").getAsString();
        } else {
            eachFieldName = null;
        }

        final List<Schema.Field> copiedInputFields = new ArrayList<>();
        for(Schema.Field field : inputFields) {
            copiedInputFields.add(field.copy());
        }

        final Schema.Field eachField;
        if(eachFieldName != null) {
            eachField = ElementSchemaUtil.getInputField(eachFieldName, inputFields);
            List<Schema.Field> childInputFields = getInputFields(eachField);
            copiedInputFields.addAll(childInputFields);
            //copiedInputFields.add(Schema.Field.of("_", eachField.getFieldType().copy()));
        } else {
            eachField = null;
        }

        final JsonElement fieldsElement = jsonObject.get("fields");
        final List<SelectFunction> childFunctions = SelectFunction.of(fieldsElement.getAsJsonArray(), copiedInputFields);
        final Schema outputSchema = SelectFunction.createSchema(childFunctions);
        final List<Schema.Field> nestedInputFields = new ArrayList<>();
        for(final SelectFunction selectFunction : childFunctions) {
            nestedInputFields.addAll(selectFunction.getInputFields());
        }
        if(eachField != null) {
            nestedInputFields.add(eachField);
        }

        final String mode;
        if(jsonObject.has("mode") && jsonObject.get("mode").isJsonPrimitive()) {
            mode = jsonObject.get("mode").getAsString();
        } else {
            mode = "nullable";
        }

        final Schema.FieldType fieldType = Schema.FieldType.element(outputSchema);
        final Schema.FieldType outputFieldType = switch (mode) {
            case "required" -> fieldType;
            case "nullable" -> fieldType.withNullable(true);
            case "repeated" -> Schema.FieldType.array(fieldType).withNullable(true);
            default -> throw new IllegalArgumentException("illegal struct mode: " + mode);
        };

        final boolean isArray = Schema.Type.array.equals(outputFieldType.getType());
        final List<Schema.Field> deduplicatedNestedInputFields = new ArrayList<>();
        final Set<String> nestedInputFieldNames = new HashSet<>();
        for(final Schema.Field nestedInputField : nestedInputFields) {
            if(!nestedInputFieldNames.contains(nestedInputField.getName())) {
                deduplicatedNestedInputFields.add(nestedInputField);
                nestedInputFieldNames.add(nestedInputField.getName());
            }
        }

        return new Struct(name, childFunctions, outputFieldType, deduplicatedNestedInputFields, eachField, eachFieldName, isArray, ignore);
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public boolean ignore() {
        return ignore;
    }

    @Override
    public List<Schema.Field> getInputFields() {
        return inputFields;
    }

    @Override
    public Schema.FieldType getOutputFieldType() {
        return outputFieldType;
    }

    @Override
    public void setup() {
        for(final SelectFunction selectFunction : this.selectFunctions) {
            selectFunction.setup();
        }
    }

    @Override
    public Object apply(Map<String, Object> input, Instant timestamp) {
        if(isArray) {
            if(eachField == null || !input.containsKey(eachField.getName())) {
                final Map<String, Object> newInput = new HashMap<>(input);
                final Map<String, Object> output = SelectFunction.apply(selectFunctions, newInput, timestamp);
                return List.of(output);
            }

            final Object eachValue = ElementSchemaUtil.getValue(input, eachFieldPath);
            final List<Object> eachValues = switch (eachValue) {
                case List list -> list;
                case String s -> {
                    try {
                        final JsonElement json = new Gson().fromJson(s, JsonElement.class);
                        final List<Object> list = new ArrayList<>();
                        if(json.isJsonArray()) {
                            for(final JsonElement e : json.getAsJsonArray()) {
                                final Object object = JsonToMapConverter.getAsPrimitiveValue(eachField.getFieldType(), e);
                                list.add(object);
                            }
                        } else {
                            throw new IllegalArgumentException("json is not json array: " + s + ", with eachField: " + eachField);
                        }
                        yield list;
                    } catch (Throwable e) {
                        throw new RuntimeException("Failed to parse each field values: " + s, e);
                    }
                }
                case null -> new ArrayList<>();
                default -> throw new IllegalArgumentException();
            };

            final List<Map<String, Object>> outputs = new ArrayList<>();
            for(final Object value : Optional.ofNullable(eachValues).orElseGet(ArrayList::new)) {
                final Map<String, Object> newInput = new HashMap<>(input);
                final Schema.FieldType elementType = switch (eachField.getFieldType().getType()) {
                    case array -> eachField.getFieldType().getArrayValueType();
                    default-> eachField.getFieldType();
                };
                switch (elementType.getType()) {
                    case element -> {
                        final Map<String,?> map = (Map) value;
                        for(Schema.Field f : elementType.getElementSchema().getFields()) {
                            newInput.put(f.getName(), map.get(f.getName()));
                        }
                    }
                    default -> newInput.put(eachField.getName(), value);
                }
                final Map<String, Object> output =  SelectFunction.apply(selectFunctions, newInput, timestamp);
                outputs.add(output);
            }
            return outputs;
        } else {
            final Map<String, Object> newInput = new HashMap<>(input);
            return SelectFunction.apply(selectFunctions, newInput, timestamp);
        }
    }

    private static List<Schema.Field> getInputFields(Schema.Field eachField) {
        return switch (eachField.getFieldType().getType()) {
            case element -> eachField.getFieldType().getElementSchema().getFields();
            case array -> switch (eachField.getFieldType().getArrayValueType().getType()) {
                case element -> eachField.getFieldType().getArrayValueType().getElementSchema().getFields();
                default -> List.of(eachField);
            };
            default -> List.of(eachField);
        };
    }

}
