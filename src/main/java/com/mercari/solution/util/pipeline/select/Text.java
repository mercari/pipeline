package com.mercari.solution.util.pipeline.select;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.mercari.solution.module.Schema;
import com.mercari.solution.util.TemplateUtil;
import com.mercari.solution.util.schema.ElementSchemaUtil;
import freemarker.template.Template;
import org.joda.time.Instant;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Text implements SelectFunction {

    private final String name;
    private final String text;
    private final List<Schema.Field> inputFields;
    private final Schema.FieldType outputFieldType;
    private final boolean ignore;

    private transient Template template;


    Text(String name, String text, List<Schema.Field> inputFields, Schema.FieldType outputFieldType, boolean ignore) {
        this.name = name;
        this.text = text;
        this.inputFields = inputFields;
        this.outputFieldType = outputFieldType;
        this.ignore = ignore;
    }

    public static Text of(String name, JsonObject jsonObject, List<Schema.Field> inputFields, boolean ignore) {

        if(!jsonObject.has("text") && !jsonObject.has("value")) {
            throw new IllegalArgumentException("Select function[" + name + "] text or value requires text parameter");
        }
        final JsonElement textElement;
        if(jsonObject.has("text")) {
            textElement = jsonObject.get("text");
        } else {
            textElement = jsonObject.get("value");
        }
        if(!textElement.isJsonPrimitive()) {
            throw new IllegalArgumentException("Select function[" + name + "].text or value parameter must be string. but: " + textElement);
        }
        final String text = textElement.getAsString();

        final List<Schema.Field> fields = new ArrayList<>();
        final List<String> templateVariables = TemplateUtil.extractTemplateArgs(text, inputFields);
        for(final String templateVariable : templateVariables) {
            final Schema.FieldType inputFieldType = ElementSchemaUtil.getInputFieldType(templateVariable, inputFields);
            if(inputFieldType == null) {
                throw new IllegalArgumentException("SelectField text: " + name + " missing inputField: " + templateVariable);
            }
            fields.add(Schema.Field.of(templateVariable, inputFieldType));
        }

        final String type;
        if(jsonObject.has("type")) {
            type = jsonObject.get("type").getAsString();
        } else {
            type = "string";
        }
        final Schema.FieldType outputFieldType = Schema.FieldType.type(Schema.Type.of(type));

        return new Text(name, text, fields, outputFieldType, ignore);
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
        this.template = TemplateUtil.createStrictTemplate(name, text);
    }

    @Override
    public Object apply(Map<String, Object> input, Instant timestamp) {
        final Map<String, Object> values = new HashMap<>(input);
        TemplateUtil.setFunctions(values);
        final String output = TemplateUtil.executeStrictTemplate(template, values);
        if(Schema.Type.string.equals(outputFieldType.getType())) {
            return output;
        } else {
            return ElementSchemaUtil.getAsPrimitive(outputFieldType, output);
        }
    }
}
