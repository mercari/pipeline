package com.mercari.solution.util.pipeline;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.mercari.solution.module.MElement;
import com.mercari.solution.module.Schema;
import com.mercari.solution.util.pipeline.select.SelectFunction;
import org.apache.beam.sdk.values.TupleTag;
import org.joda.time.Instant;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class Partition implements Serializable {

    private final String name;

    private final Filter filter;
    private final Select select;
    private final Unnest unnest;

    private final Schema outputSchema;
    private final TupleTag<MElement> outputTag;


    public String getName() {
        return name;
    }

    public Schema getOutputSchema() {
        return outputSchema;
    }

    public TupleTag<MElement> getOutputTag() {
        return outputTag;
    }

    private Partition(
            final String name,
            final Filter filter,
            final Select select,
            final Unnest unnest,
            final Schema inputSchema) {

        this.name = name;
        this.filter = filter;
        this.select = select;
        this.unnest = unnest;
        this.outputSchema = createOutputSchema(inputSchema, select, unnest);
        this.outputTag = new TupleTag<>() {};
    }

    public static List<Partition> of(
            final JsonArray partitionArray,
            final Schema inputSchema) {

        final List<Partition> partitions = new ArrayList<>();
        if(partitionArray == null || !partitionArray.isJsonArray()) {
            return partitions;
        }

        for(final JsonElement jsonElement : partitionArray) {
            if(!jsonElement.isJsonObject()) {
                continue;
            }
            final Partition partition = of(jsonElement.getAsJsonObject(), inputSchema);
            partitions.add(partition);
        }
        return partitions;
    }

    public static Partition of(
            final JsonObject jsonObject,
            final Schema inputSchema) {

        final String name;
        if(jsonObject.has("name")) {
            name = jsonObject.get("name").getAsString();
        } else {
            name = null;
        }

        final JsonElement filterJson;
        if(jsonObject.has("filter")) {
            filterJson = jsonObject.get("filter");
        } else {
            filterJson = null;
        }
        final Filter filter = Filter.of(filterJson);

        final JsonArray selectJson;
        if(jsonObject.has("select")) {
            selectJson = jsonObject.getAsJsonArray("select");
        } else {
            selectJson = null;
        }
        final Select select = Select.of(selectJson, inputSchema.getFields());

        final String flattenField;
        if(jsonObject.has("flattenField")) {
            flattenField = jsonObject.get("flattenField").getAsString();
        } else {
            flattenField = null;
        }
        final Unnest unnest = Unnest.of(flattenField);

        return new Partition(name, filter, select, unnest, inputSchema);
    }

    public static Partition of(
            final String name,
            final JsonElement filterJson,
            final JsonArray selectJson,
            final String flattenField,
            final Schema inputSchema) {

        final Filter filter = Filter.of(filterJson);
        final Select select = Select.of(selectJson, inputSchema.getFields());
        final Unnest unnest = Unnest.of(flattenField);

        return new Partition(name, filter, select, unnest, inputSchema);
    }

    private List<String> validate(final Schema schema) {
        final List<String> errorMessages = new ArrayList<>();

        return errorMessages;
    }

    public void setup() {
        if(filter != null) {
            filter.setup();
        }
        if(select != null && select.useSelect()) {
            select.setup();
        }
        if(unnest != null && unnest.useUnnest()) {
            unnest.setup();
        }
    }

    public static Schema createOutputSchema(
            final Schema inputSchema,
            final Select select,
            final Unnest unnest) {

        if(!select.useSelect() && !unnest.useUnnest()) {
            return inputSchema;
        } else if(select.useSelect()) {
            return SelectFunction
                    .createSchema(select.getSelectFunctions(), unnest.getFlattenField());
        } else if(unnest.useUnnest()) {
            return Unnest.createSchema(inputSchema.getFields(), unnest.getFlattenField());
        } else {
            throw new IllegalStateException("Could not create output schema for partition");
        }
    }

    public List<MElement> execute(
            final MElement input,
            final Instant timestamp) {

        final List<MElement> outputs = new ArrayList<>();
        if (!filter.filter(input)) {
            return outputs;
        }

        if (!select.useSelect() && !unnest.useUnnest()) {
            outputs.add(input);
        } else if(select.useSelect()) {
            final Map<String, Object> primitiveValues = select.select(input, timestamp);
            if(unnest.useUnnest()) {
                final List<Map<String, Object>> list = unnest.unnest(primitiveValues);
                outputs.addAll(MElement.ofList(list, timestamp));
            } else {
                final MElement output = MElement.of(primitiveValues, timestamp);
                outputs.add(output);
            }
        } else {
            final List<Map<String, Object>> list = unnest.unnest(input);
            outputs.addAll(MElement.ofList(list, timestamp));
        }

        return outputs;
    }

}
