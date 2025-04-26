package com.mercari.solution.module.transform;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.mercari.solution.module.*;
import com.mercari.solution.util.pipeline.Filter;
import com.mercari.solution.util.pipeline.Select;
import com.mercari.solution.util.pipeline.Union;
import com.mercari.solution.util.pipeline.select.SelectFunction;
import org.apache.beam.sdk.values.*;

import java.io.Serializable;
import java.util.*;


@Transform.Module(name="filter")
public class FilterTransform extends Transform {

    private static class Parameters implements Serializable {

        private JsonElement filter;
        private JsonArray select;
        private String flattenField;
        private DataType outputType;

        // deprecated
        private JsonElement filters;

        public boolean useFilter() {
            return filter != null && (filter.isJsonObject() || filter.isJsonArray());
        }

        public boolean useSelect() {
            return select != null && select.isJsonArray();
        }

        private void validate() {
            final List<String> errorMessages = new ArrayList<>();
            if((this.filter == null || this.filter.isJsonNull())
                    && (this.select == null || !this.select.isJsonArray())) {

                if(this.filters == null || this.filters.isJsonNull()) {
                    errorMessages.add("Filter transform module parameters must contain filter or select parameter.");
                }
            }

            if(!errorMessages.isEmpty()) {
                throw new IllegalModuleException(errorMessages);
            }
        }

        private void setDefaults() {
            if(outputType == null) {
                outputType = DataType.ELEMENT;
            }
            if(filter == null || filter.isJsonNull()) {
                if(filters != null && !filters.isJsonNull()) {
                    this.filter = this.filters;
                }
            }
        }
    }

    @Override
    public MCollectionTuple expand(
            final MCollectionTuple inputs,
            final MErrorHandler errorHandler) {

        final Parameters parameters = getParameters(Parameters.class);
        parameters.validate();
        parameters.setDefaults();

        final PCollection<MElement> input = inputs
                .apply("Union", Union.flatten()
                        .withWaits(getWaits())
                        .withStrategy(getStrategy()));
        final Schema inputSchema = Union.createUnionSchema(inputs);

        final Schema outputSchema;
        final PCollection<MElement> output;
        final PCollection<MElement> failure;
        if(parameters.useSelect()) {
            final List<SelectFunction> selectFunctions = SelectFunction.of(parameters.select, inputSchema.getFields());
            final Select.Transform selectTransform = Select
                    .of(getJobName(), getName(), parameters.filter, selectFunctions, parameters.flattenField, getLoggings(), getFailFast(), parameters.outputType);
            outputSchema = selectTransform.outputSchema;
            final String stepName = (parameters.useFilter() ? "FilterAnd" : "") + "Select";
            final PCollectionTuple selectedTuple = input.apply(stepName, selectTransform);
            output = selectedTuple.get(selectTransform.outputTag);
            failure = selectedTuple.has(selectTransform.failuresTag) ? selectedTuple.get(selectTransform.failuresTag) : null;
        } else {
            final Filter.Transform filteringTransform = Filter.of(getJobName(), getName(), parameters.filter, inputSchema, getLoggings(), getFailFast());
            outputSchema = inputSchema;
            final PCollectionTuple filteredTuple = input.apply("Filter", filteringTransform);
            output = filteredTuple.get(filteringTransform.outputTag);
            failure = filteredTuple.has(filteringTransform.failuresTag) ? filteredTuple.get(filteringTransform.failuresTag) : null;
        }

        return MCollectionTuple
                .of(output, outputSchema);
    }

}
