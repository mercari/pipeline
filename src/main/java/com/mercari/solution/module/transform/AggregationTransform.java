package com.mercari.solution.module.transform;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.mercari.solution.module.*;
import com.mercari.solution.util.coder.ElementCoder;
import com.mercari.solution.util.pipeline.*;
import com.mercari.solution.util.pipeline.Filter;
import com.mercari.solution.util.pipeline.aggregation.Aggregators;
import com.mercari.solution.util.pipeline.select.SelectFunction;
import org.apache.beam.sdk.coders.*;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.*;


@Transform.Module(name="aggregation")
public class AggregationTransform extends Transform {

    private static final Logger LOG = LoggerFactory.getLogger(AggregationTransform.class);

    private static class AggregateTransformParameters implements Serializable {

        private List<String> groupFields;
        private JsonElement filter;
        private JsonArray select;
        private String flattenField;

        private List<AggregationDefinition> aggregations;
        private Limit.LimitParameters limit;

        private Integer fanout;

        private Boolean outputEmpty;
        private Boolean outputPaneInfo;

        // deprecated. for compatibility. use strategy
        private Strategy.WindowStrategy window;
        private Strategy.TriggerStrategy trigger;
        private Strategy.AccumulationMode accumulationMode;

        public boolean useFilter() {
            return filter != null && (filter.isJsonObject() || filter.isJsonArray());
        }

        public boolean useSelect() {
            return select != null && select.isJsonArray();
        }


        public void validate(Map<String, Schema> inputSchemas) {
            final List<String> errorMessages = new ArrayList<>();
            if(groupFields != null && !groupFields.isEmpty()) {
                for(final Map.Entry<String, Schema> entry : inputSchemas.entrySet()) {
                    for(final String groupField : groupFields) {
                        if(!entry.getValue().hasField(groupField)) {
                            errorMessages.add("parameter groupFields[" + groupField + "] does not exists in schema from input: " + entry.getKey());
                        }
                    }
                }
            }
            if(this.aggregations == null || this.aggregations.isEmpty()) {
                errorMessages.add("aggregations parameter must not be null or size zero.");
            } else {
                for(int index=0; index < this.aggregations.size(); index++) {
                    final AggregationDefinition definition = this.aggregations.get(index);
                    errorMessages.addAll(definition.validate(inputSchemas, index));
                }
            }

            if(this.select != null && !this.select.isJsonArray()) {
                errorMessages.add("select parameter must be array.");
            }
            if(this.filter != null && !this.filter.isJsonNull()) {
                if(this.filter.isJsonPrimitive()) {
                    errorMessages.add("filter parameter must be array or object.");
                }
            }
            if(this.limit != null) {
                errorMessages.addAll(this.limit.validate());
            }

            if(!errorMessages.isEmpty()) {
                throw new IllegalModuleException(errorMessages);
            }
        }

        public void setDefaults() {
            if(this.limit != null) {
                this.limit.setDefaults();
            }
            if(this.outputEmpty == null) {
                this.outputEmpty = false;
            }
            if(this.outputPaneInfo == null) {
                this.outputPaneInfo = false;
            }
        }
    }

    private static class AggregationDefinition implements Serializable {

        private String input;
        private JsonArray fields;

        public List<String> validate(Map<String, Schema> inputSchemas, int index) {
            final List<String> errorMessages = new ArrayList<>();
            if(this.input == null) {
                errorMessages.add("aggregations[" + index + "].input parameter must not be null.");
            } else if(!inputSchemas.containsKey(this.input)) {
                errorMessages.add("aggregations[" + index + "].input[" + this.input + "] not found in inputs: " + inputSchemas.keySet());
            }
            return errorMessages;
        }

    }

    @Override
    public MCollectionTuple expand(MCollectionTuple inputs) {

        final Map<String, Schema> inputSchemas = inputs.getAllSchemaAsMap();
        final AggregateTransformParameters parameters = getParameters(AggregateTransformParameters.class);

        Strategy strategy = getStrategy();
        if(strategy.isDefault() && (parameters.window != null || parameters.trigger != null)) {
            strategy = Strategy.of(parameters.window, parameters.trigger, parameters.accumulationMode);
        }
        strategy.setDefaults();

        parameters.validate(inputSchemas);
        parameters.setDefaults();

        final List<Aggregators> aggregatorsList = new ArrayList<>();
        for(final AggregationDefinition definition : parameters.aggregations) {
            final Schema inputSchema = inputSchemas.get(definition.input);
            aggregatorsList.add(Aggregators.of(definition.input, parameters.groupFields, inputSchema, definition.fields));
        }

        final Schema aggregationOutputSchema = Aggregation.createOutputSchema(
                inputSchemas, parameters.groupFields, aggregatorsList);

        final PCollection<KV<String,Map<String,Object>>> aggregated = inputs
                .apply("Aggregate", Aggregation.aggregate(
                        inputs.getAllInputs(),
                        parameters.groupFields,
                        strategy,
                        aggregatorsList,
                        parameters.fanout));

        final String filterJson;
        if(parameters.filter == null || parameters.filter.isJsonNull()) {
            filterJson = null;
        } else {
            filterJson = parameters.filter.toString();
        }

        final Schema outputSchema;
        final List<SelectFunction> selectFunctions = SelectFunction.of(parameters.select, aggregationOutputSchema.getFields());
        if(selectFunctions.isEmpty()) {
            outputSchema = aggregationOutputSchema;
        } else {
            outputSchema = SelectFunction.createSchema(selectFunctions, parameters.flattenField);
        }

        final PCollection<KV<String, MElement>> formatted = aggregated
                .apply("Format", ParDo.of(new FormatDoFn(outputSchema, filterJson, selectFunctions)))
                .setCoder(KvCoder.of(StringUtf8Coder.of(), ElementCoder.of(outputSchema)));

        final PCollection<MElement> limited;
        if(parameters.limit != null) {
            limited = formatted
                    .apply("Limit", Limit
                            .of(parameters.limit, List.of(outputSchema), OptionUtil.isStreaming(inputs)));
        } else {
            limited = formatted
                    .apply("Values", Values.create());
        }

        return MCollectionTuple.of(limited, outputSchema);
    }

    private static class FormatDoFn extends DoFn<KV<String,Map<String,Object>>, KV<String,MElement>> {

        private final Schema schema;
        private final String filterJson;
        private final List<SelectFunction> selectFunctions;

        private transient Filter.ConditionNode conditionNode;

        FormatDoFn(
                final Schema schema,
                final String filterJson,
                final List<SelectFunction> selectFunctions) {

            this.schema = schema;
            this.filterJson = filterJson;
            this.selectFunctions = selectFunctions;
        }

        @Setup
        public void setup() {
            schema.setup();
            if(filterJson != null) {
                conditionNode = Filter.parse(filterJson);
            }
            for(final SelectFunction selectFunction : selectFunctions) {
                selectFunction.setup();
            }
        }

        @ProcessElement
        public void processElement(ProcessContext c) {
            final Map<String,Object> values = c.element().getValue();
            if(conditionNode != null && !Filter.filter(conditionNode, values)) {
                return;
            }
            final Map<String,Object> result = Select.apply(selectFunctions, values, c.timestamp());
            final MElement output = MElement.of(schema, result, c.timestamp());
            c.output(KV.of(c.element().getKey(), output));
        }

    }

}
