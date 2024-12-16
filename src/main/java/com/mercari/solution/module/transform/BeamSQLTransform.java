package com.mercari.solution.module.transform;

import com.mercari.solution.module.*;
import com.mercari.solution.util.TemplateUtil;
import com.mercari.solution.util.coder.ElementCoder;
import com.mercari.solution.util.gcp.StorageUtil;
import com.mercari.solution.util.schema.converter.ElementToRowConverter;
import com.mercari.solution.util.sql.udf.AggregateFunctions;
import com.mercari.solution.util.sql.udf.ArrayFunctions;
import com.mercari.solution.util.sql.udf.MathFunctions;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.extensions.sql.SqlTransform;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Transform.Module(name="beamsql")
public class BeamSQLTransform extends Transform {

    private static final Logger LOG = LoggerFactory.getLogger(BeamSQLTransform.class);

    private static class BeamSQLTransformParameters implements Serializable {

        private String sql;
        private String ddl;
        private Map<String, String> namedParameters;
        private List<String> positionalParameters;
        private Boolean autoLoading;
        private Planner planner;

        private void validate() {
            if(this.sql == null) {
                throw new IllegalModuleException("parameters.sql must not be null");
            }
        }

        private void setDefaults(Map<String,Object> templateArgs) {
            sql = loadQuery(sql, templateArgs);
            ddl = loadQuery(ddl, templateArgs);
            if(namedParameters == null) {
                namedParameters = new HashMap<>();
            }
            if(positionalParameters == null) {
                positionalParameters = new ArrayList<>();
            }
        }

        private String loadQuery(String sql, Map<String, Object> templateArgs) {
            if(sql == null) {
                return null;
            }
            String query;
            if(sql.startsWith("gs://")) {
                final String rawQuery = StorageUtil.readString(sql);
                query = TemplateUtil.executeStrictTemplate(rawQuery, templateArgs);
            } else {
                if(Files.exists(Paths.get(sql)) && !Files.isDirectory(Paths.get(sql))) {
                    try {
                        final String rawQuery = Files.readString(Paths.get(sql), StandardCharsets.UTF_8);
                        query = TemplateUtil.executeStrictTemplate(rawQuery, templateArgs);
                    } catch (IOException e) {
                        query = sql;
                    }
                } else {
                    query = sql;
                }
            }
            return query;
        }
    }

    public enum Planner {
        zetasql,
        calcite
    }

    @Override
    public MCollectionTuple expand(MCollectionTuple inputs) {

        final BeamSQLTransformParameters parameters = getParameters(BeamSQLTransformParameters.class);
        parameters.validate();
        parameters.setDefaults(getTemplateArgs());

        PCollectionTuple tuple = PCollectionTuple.empty(inputs.getPipeline());
        for(final Map.Entry<String, MCollection> entry : inputs.asCollectionMap().entrySet()) {
            final Schema schema = entry.getValue().getSchema();
            schema.setup();
            PCollection<Row> row = entry.getValue()
                    .apply("ConvertRow" + entry.getKey(), ParDo.of(new ConvertRowDoFn(schema)))
                    .setCoder(RowCoder.of(schema.getRowSchema()))
                    .setRowSchema(schema.getRowSchema());
            if(getStrategy() != null) {
                row = row.apply("WithWindow", getStrategy().createWindow());
            }
            tuple = tuple.and(entry.getKey(), row);
        }

        SqlTransform transform = SqlTransform.query(parameters.sql);
        transform = switch (parameters.planner) {
            case zetasql -> transform.withQueryPlannerClass(org.apache.beam.sdk.extensions.sql.zetasql.ZetaSQLQueryPlanner.class);
            case null, default -> transform.withQueryPlannerClass(org.apache.beam.sdk.extensions.sql.impl.CalciteQueryPlanner.class);
        };

        if(parameters.ddl != null) {
            transform = transform.withDdlString(parameters.ddl);
        }
        if(!parameters.namedParameters.isEmpty()) {
            transform = transform.withNamedParameters(parameters.namedParameters);
        } else if(!parameters.positionalParameters.isEmpty()) {
            transform = transform.withPositionalParameters(parameters.positionalParameters);
        }
        if(parameters.autoLoading != null) {
            transform = transform.withAutoLoading(parameters.autoLoading);
        }

        final PCollection<Row> output = tuple.apply("SQLTransform", transform
                // Math UDFs
                .registerUdf("MDT_GREATEST_INT64", MathFunctions.GreatestInt64Fn.class)
                .registerUdf("MDT_GREATEST_FLOAT64", MathFunctions.GreatestFloat64Fn.class)
                .registerUdf("MDT_LEAST_INT64", MathFunctions.LeastInt64Fn.class)
                .registerUdf("MDT_LEAST_FLOAT64", MathFunctions.LeastFloat64Fn.class)
                .registerUdf("MDT_GENERATE_UUID", MathFunctions.GenerateUUIDFn.class)
                // Array UDFs
                .registerUdf("MDT_CONTAINS_ALL_INT64", ArrayFunctions.ContainsAllInt64sFn.class)
                .registerUdf("MDT_CONTAINS_ALL_STRING", ArrayFunctions.ContainsAllStringsFn.class)
                // UDAFs
                .registerUdaf("MDT_ARRAY_AGG_INT64", new AggregateFunctions.ArrayAggInt64Fn())
                .registerUdaf("MDT_ARRAY_AGG_STRING", new AggregateFunctions.ArrayAggStringFn())
                .registerUdaf("MDT_ARRAY_AGG_DISTINCT_STRING", new AggregateFunctions.ArrayAggDistinctStringFn())
                .registerUdaf("MDT_ARRAY_AGG_DISTINCT_FLOAT64", new AggregateFunctions.ArrayAggDistinctFloat64Fn())
                .registerUdaf("MDT_ARRAY_AGG_DISTINCT_INT64", new AggregateFunctions.ArrayAggDistinctInt64Fn())
                .registerUdaf("MDT_COUNT_DISTINCT_STRING", new AggregateFunctions.CountDistinctStringFn())
                .registerUdaf("MDT_COUNT_DISTINCT_FLOAT64", new AggregateFunctions.CountDistinctFloat64Fn())
                .registerUdaf("MDT_COUNT_DISTINCT_INT64", new AggregateFunctions.CountDistinctInt64Fn())
        );

        final Schema outputSchema = Schema.of(output.getSchema());
        final PCollection<MElement> element = output
                .apply("ConvertElement", ParDo.of(new ConvertElementDoFn()))
                .setCoder(ElementCoder.of(outputSchema));

        return MCollectionTuple
                .of(element, Schema.of(output.getSchema()));
    }

    private static class ConvertRowDoFn extends DoFn<MElement, Row> {

        private final Schema schema;

        ConvertRowDoFn(Schema schema) {
            this.schema = schema;
        }

        @Setup
        public void setup() {
            this.schema.setup();
        }

        @ProcessElement
        public void processElement(ProcessContext c) {
            final MElement element = c.element();
            if(element == null) {
                return;
            }
            final Row row = ElementToRowConverter.convert(schema, element);
            c.output(row);
        }

    }

    private static class ConvertElementDoFn extends DoFn<Row, MElement> {

        @ProcessElement
        public void processElement(ProcessContext c) {
            final Row row = c.element();
            if(row == null) {
                return;
            }
            final MElement element = MElement.of(row, c.timestamp());
            c.output(element);
        }

    }

}
