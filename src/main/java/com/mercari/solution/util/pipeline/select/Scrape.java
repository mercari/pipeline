package com.mercari.solution.util.pipeline.select;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.mercari.solution.module.Schema;
import com.mercari.solution.util.DateTimeUtil;
import com.mercari.solution.util.schema.ElementSchemaUtil;
import org.joda.time.Instant;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Scrape implements SelectFunction {

    private static final Logger LOG = LoggerFactory.getLogger(Scrape.class);

    private final String name;
    private final String field;
    private final String selector;
    private final String mode;

    private final String attribute;
    private final String patternText;
    private final Integer group;
    private final String baseUri;
    private final Boolean trim;
    private final List<Scrape> fields;

    private final List<Schema.Field> inputFields;
    private final Schema.FieldType outputFieldType;
    private final boolean ignore;

    private transient Pattern pattern;

    Scrape(String name,
           String field,
           String selector,
           String mode,
           String attribute,
           String patternText,
           Integer group,
           String baseUri,
           Boolean trim,
           List<Scrape> fields,
           List<Schema.Field> inputFields,
           Schema.FieldType outputFieldType,
           boolean ignore) {

        this.name = name;
        this.field = field;
        this.selector = selector;
        this.mode = mode;

        this.attribute = attribute;
        this.patternText = patternText;
        this.group = group;
        this.baseUri = baseUri;
        this.trim = trim;
        this.fields = fields;

        this.inputFields = inputFields;
        this.outputFieldType = outputFieldType;
        this.ignore = ignore;
    }

    public static Scrape of(String name, JsonObject jsonObject, List<Schema.Field> inputFields, boolean ignore) {

        //if(!jsonObject.has("field")) {
        //    throw new IllegalArgumentException("SelectField scrape: " + name + " requires field parameter");
        //}
        //final String field = jsonObject.get("field").getAsString();
        final String field = SelectFunction.getStringParameter(name, jsonObject, "field", null);

        if(!jsonObject.has("selector")) {
            throw new IllegalArgumentException("SelectField scrape: " + name + " requires selector parameter");
        }
        final String selector = jsonObject.get("selector").getAsString();

        final String mode = SelectFunction.getStringParameter(name, jsonObject, "mode", "nullable");
        final String type = SelectFunction.getStringParameter(name, jsonObject, "type", "string");

        final String attribute = SelectFunction.getStringParameter(name, jsonObject, "attribute", null);
        final String patternText = SelectFunction.getStringParameter(name, jsonObject, "pattern", null);

        final Integer group;
        if(jsonObject.has("group")) {
            if(!jsonObject.get("group").isJsonPrimitive() || !jsonObject.get("group").getAsJsonPrimitive().isNumber()) {
                throw new IllegalArgumentException("SelectField scrape: " + name + ".group parameter must be integer");
            }
            group = jsonObject.get("group").getAsInt();
        } else {
            group = null;
        }

        final String baseUri = SelectFunction.getStringParameter(name, jsonObject, "baseUri", null);

        final boolean trim;
        if(jsonObject.has("trim")) {
            if(!jsonObject.get("trim").isJsonPrimitive() || !jsonObject.get("trim").getAsJsonPrimitive().isBoolean()) {
                throw new IllegalArgumentException("SelectField scrape: " + name + ".trim parameter must be boolean");
            }
            trim = jsonObject.get("trim").getAsBoolean();
        } else {
            trim = false;
        }

        final Schema.FieldType outputFieldType;

        final List<Scrape> fields = new ArrayList<>();
        if(jsonObject.has("fields") && jsonObject.get("fields").isJsonArray()) {
            final List<Schema.Field> childFields = new ArrayList<>();
            for(final JsonElement element : jsonObject.getAsJsonArray("fields")) {
                if(!element.isJsonObject()) {
                    continue;
                }
                final JsonObject child = element.getAsJsonObject();
                if(!child.has("name")) {
                    throw new IllegalArgumentException("select requires name parameter");
                }
                final String childName = child.get("name").getAsString();

                final boolean childIgnore;
                if(child.has("ignore")) {
                    childIgnore = child.get("ignore").getAsBoolean();
                } else {
                    childIgnore = false;
                }

                final Scrape scrape = Scrape.of(childName, child, inputFields, childIgnore);
                fields.add(scrape);
                final Schema.Field childField = Schema.Field.of(childName, scrape.getOutputFieldType());
                childFields.add(childField);
            }
            outputFieldType = Schema.FieldType
                    .element(Schema.builder().withFields(childFields).build())
                    .withNullable(true);
        } else {
            outputFieldType = Schema.FieldType.type(type);
        }

        final List<Schema.Field> scrapeInputFields = new ArrayList<>();
        if(field != null && inputFields != null) {
            final boolean fieldIsInInputFields = inputFields.stream().anyMatch(f -> field.equals(f.getName()));
            if(fieldIsInInputFields) {
                final Schema.FieldType inputFieldType = ElementSchemaUtil.getInputFieldType(field, inputFields);
                if(inputFieldType == null) {
                    throw new IllegalArgumentException("SelectField scrape: " + name + " missing inputField: " + field + " in input fields: " + inputFields);
                }
                scrapeInputFields.add(Schema.Field.of(field, inputFieldType));
            }
        }

        return new Scrape(
                name, field, selector, mode,
                attribute, patternText, group, baseUri, trim, fields,
                scrapeInputFields, outputFieldType, ignore);
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
        if("repeated".equalsIgnoreCase(mode)) {
            return Schema.FieldType.array(outputFieldType);
        } else {
            return outputFieldType;
        }
    }

    @Override
    public void setup() {
        if(patternText != null) {
            this.pattern = Pattern.compile(patternText);
        }
    }

    @Override
    public Object apply(Map<String, Object> input, Instant timestamp) {
        if(input == null || !input.containsKey(field)) {
            return null;
        }
        final Object fieldValue = input.get(field);
        if(fieldValue == null) {
            return null;
        }
        final String html = fieldValue.toString();
        final Document document = Jsoup.parse(html);
        if(baseUri != null) {
            document.setBaseUri(baseUri);
        }
        final Elements elements = document.select(selector);
        if("repeated".equalsIgnoreCase(mode)) {
            final List<Object> outputs = new ArrayList<>();
            if(elements.isEmpty()) {
                return outputs;
            }
            for(final Element element : elements) {
                final Object output = extract(element, this);
                if(output != null) {
                    outputs.add(output);
                }
            }
            return outputs;
        } else {
            if(elements.isEmpty()) {
                return null;
            }
            final Element element = elements.get(0);
            return extract(element, this);
        }
    }

    private static Object extract(final Element element, final Scrape scrape) {

        if(Schema.Type.element.equals(scrape.outputFieldType.getType())) {
            final Map<String, Object> values = new HashMap<>();
            for(final Scrape field : scrape.fields) {
                final Elements childElements = element.select(field.selector);
                if("repeated".equalsIgnoreCase(field.mode)) {
                    final List<Object> childValues = new ArrayList<>();
                    for(final Element childElement : childElements) {
                        final Object childValue = extract(childElement, field);
                        childValues.add(childValue);
                    }
                    values.put(field.getName(), childValues);
                } else {
                    final Object childValue;
                    if(childElements.isEmpty()) {
                        childValue = null;
                    } else {
                        childValue = extract(childElements.get(0), field);
                    }
                    values.put(field.getName(), childValue);
                }
            }
            return values;
        }

        String text;
        if(scrape.attribute == null) {
            text = element.text();
        } else {
            text = element.attr(scrape.attribute);
        }
        if(scrape.pattern != null) {
            final Matcher matcher = scrape.pattern.matcher(text);
            if(matcher.find()) {
                if(scrape.group != null) {
                    if(matcher.groupCount() < scrape.group) {
                        text = null;
                    } else {
                        text = matcher.group(scrape.group);
                    }
                } else {
                    text = matcher.group();
                }
            } else {
                text = null;
            }
        }

        if(text == null) {
            return null;
        }

        final Object output = switch (scrape.outputFieldType.getType()) {
            case string -> scrape.trim ? text.trim() : text;
            case bool -> Boolean.parseBoolean(text.trim());
            case int32 -> Integer.parseInt(text.trim().replaceAll(",", ""));
            case int64 -> Long.parseLong(text.trim().replaceAll(",", ""));
            case float32 -> Float.parseFloat(text.trim().replaceAll(",", ""));
            case float64 -> Double.parseDouble(text.trim().replaceAll(",", ""));
            case bytes -> Base64.getDecoder().decode(text.getBytes(StandardCharsets.UTF_8));
            case date -> Long.valueOf(DateTimeUtil.toLocalDate(text.trim()).toEpochDay()).intValue();
            case time -> DateTimeUtil.toLocalTime(text.trim()).toNanoOfDay() / 1000L;
            case enumeration -> scrape.outputFieldType.getSymbolIndex(text.trim());
            default -> throw new IllegalArgumentException("Not supported type: " + scrape.outputFieldType.getType());
        };

        return output;
    }

}
