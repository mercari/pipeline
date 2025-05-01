package com.mercari.solution.util.pipeline.processing.processor.utility;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.mercari.solution.util.DateTimeUtil;
import com.mercari.solution.util.pipeline.Filter;
import com.mercari.solution.util.pipeline.processing.ProcessingBuffer;
import com.mercari.solution.util.pipeline.processing.ProcessingState;
import com.mercari.solution.util.pipeline.processing.processor.Processor;
import org.apache.beam.sdk.extensions.sql.impl.utils.CalciteUtils;
import org.apache.beam.sdk.schemas.Schema;
import org.joda.time.Instant;

import java.util.*;

public class Constant implements Processor {

    private final String name;
    private final String type;
    private final String valueElement;

    private final String condition;
    private final Boolean ignore;

    private final Map<String, Integer> bufferSizes;

    private transient Object value;
    private transient Filter.ConditionNode conditionNode;


    public Constant(final String name, final String condition, final Boolean ignore, final JsonObject params) {

        this.name = name;
        this.condition = condition;
        this.ignore = ignore != null && ignore;

        if(!params.has("type") || !params.get("type").isJsonPrimitive()) {
            throw new IllegalArgumentException("Constant step: " + name + " requires type");
        }
        this.type = params.get("type").getAsString();

        if(!params.has("value") || !params.get("value").isJsonPrimitive()) {
            throw new IllegalArgumentException("Constant step: " + name + " requires value");
        }
        this.valueElement = params.get("type").toString();

        this.bufferSizes = new HashMap<>();
        this.bufferSizes.put(name, 0);
    }

    public static Constant of(final String name, final String condition, final Boolean ignore, final JsonObject params) {
        return new Constant(name, condition, ignore, params);
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public Op getOp() {
        return Op.binning;
    }

    @Override
    public boolean ignore() {
        return ignore;
    }

    @Override
    public boolean filter(Map<String, Object> input) {
        if(condition == null) {
            return true;
        }
        return Filter.filter(this.conditionNode, input);
    }

    @Override
    public SizeUnit getSizeUnit() {
        return SizeUnit.count;
    }

    @Override
    public List<Schema.Field> getOutputFields(Map<String, Schema.FieldType> inputTypes) {
        final Schema.FieldType fieldType;
        switch (type) {
            case "string":
                fieldType = Schema.FieldType.STRING.withNullable(true);
                break;
            case "boolean":
                fieldType = Schema.FieldType.BOOLEAN.withNullable(true);
                break;
            case "int16":
                fieldType = Schema.FieldType.INT16.withNullable(true);
                break;
            case "int":
            case "int32":
            case "integer":
                fieldType = Schema.FieldType.INT32.withNullable(true);
                break;
            case "int64":
            case "long":
                fieldType = Schema.FieldType.INT64.withNullable(true);
                break;
            case "float":
            case "float32":
                fieldType = Schema.FieldType.FLOAT.withNullable(true);
                break;
            case "double":
            case "float64":
                fieldType = Schema.FieldType.DOUBLE.withNullable(true);
                break;
            case "decimal":
            case "numeric":
                fieldType = Schema.FieldType.DECIMAL.withNullable(true);
                break;
            case "date":
                fieldType = CalciteUtils.DATE.withNullable(true);
                break;
            case "time":
                fieldType = CalciteUtils.TIME.withNullable(true);
                break;
            case "timestamp":
                fieldType = Schema.FieldType.DATETIME.withNullable(true);
                break;
            default:
                throw new IllegalArgumentException("Constant not supported type: " + type);

        }

        final List<Schema.Field> outputFields = new ArrayList<>();
        outputFields.add(Schema.Field.of(name, fieldType));
        return outputFields;
    }

    @Override
    public Map<String, Schema.FieldType> getBufferTypes(Map<String, Schema.FieldType> inputTypes) {
        final Map<String, Schema.FieldType> bufferTypes = new HashMap<>();
        for(final String input : bufferSizes.keySet()) {
            final Schema.FieldType inputType = inputTypes.get(input);
            if(inputType != null) {
                bufferTypes.put(input, inputType);
            } else if(name.equals(input)) {
                bufferTypes.put(input, Schema.FieldType.DOUBLE.withNullable(true));
            } else {
                throw new IllegalArgumentException("expression processor: " + name + " has no inputType for input: " + input);
            }
        }
        return bufferTypes;
    }

    @Override
    public Map<String, Integer> getBufferSizes() {
        return this.bufferSizes;
    }

    @Override
    public void setup() {
        if(this.condition != null && this.conditionNode == null) {
            this.conditionNode = Filter.parse(new Gson().fromJson(this.condition, JsonElement.class));
        }

        if(valueElement == null) {
            this.value = null;
        } else {
            final JsonElement element = new Gson().fromJson(valueElement, JsonElement.class);
            if(element.isJsonNull()) {
                this.value = null;
            } else {
                switch (type) {
                    case "string":
                        this.value = element.getAsString();
                        break;
                    case "boolean":
                        this.value = element.getAsBoolean();
                        break;
                    case "int16":
                        this.value = element.getAsShort();
                        break;
                    case "int":
                    case "int32":
                    case "integer":
                        this.value = element.getAsInt();
                        break;
                    case "int64":
                    case "long":
                        this.value = element.getAsLong();
                        break;
                    case "float":
                    case "float32":
                        this.value = element.getAsFloat();
                        break;
                    case "double":
                    case "float64":
                        this.value = element.getAsDouble();
                        break;
                    case "decimal":
                    case "numeric":
                        this.value = element.getAsBigDecimal();
                        break;
                    case "date":
                        this.value = Long.valueOf(DateTimeUtil.toLocalDate(element.getAsString()).toEpochDay()).intValue();
                        break;
                    case "time":
                        this.value = DateTimeUtil.toLocalTime(element.getAsString()).toNanoOfDay() / 1000L;
                        break;
                    case "timestamp":
                        this.value = DateTimeUtil.toJodaInstant(element.getAsString()).getMillis() * 1000L;
                        break;
                    default:
                        throw new IllegalArgumentException("Constant not supported type: " + type);
                }
            }

        }

    }

    @Override
    public Map<String, Object> process(ProcessingBuffer buffer, ProcessingState state, Instant timestamp) {
        final Map<String, Object> outputs = new HashMap<>();
        outputs.put(name, value);
        return outputs;
    }

    @Override
    public Map<String, Object> process(Map<String, Object> input, Instant timestamp) {
        final Map<String, Object> outputs = new HashMap<>();
        outputs.put(name, value);
        return outputs;
    }

    @Override
    public List<Map<String, Object>> process(List<Map<String, Object>> inputs, Instant timestamp) {
        final List<Map<String, Object>> outputs = new ArrayList<>();
        for(final Map<String, Object> input : inputs) {
            final Map<String, Object> output = new HashMap<>();
            output.put(name, value);
            outputs.add(output);
        }
        return outputs;
    }

}
