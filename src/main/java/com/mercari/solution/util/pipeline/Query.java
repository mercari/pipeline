package com.mercari.solution.util.pipeline;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.mercari.solution.module.MElement;
import com.mercari.solution.module.MFailure;
import com.mercari.solution.module.Schema;
import com.mercari.solution.util.coder.ElementCoder;
import com.mercari.solution.util.pipeline.aggregation.Aggregator;
import com.mercari.solution.util.pipeline.select.SelectFunction;
import com.mercari.solution.util.schema.CalciteSchemaUtil;
import com.mercari.solution.util.sql.calcite.MemorySchema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;

import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.config.Lex;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.jdbc.CalciteConnection;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.rel.RelNode;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.rel.RelRoot;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.rel.RelWriter;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.rel.externalize.RelWriterImpl;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.schema.SchemaPlus;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.sql.SqlExplainLevel;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.sql.SqlNode;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.sql.parser.SqlParseException;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.sql.parser.SqlParser;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.tools.*;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Query {

    private static final Logger LOG = LoggerFactory.getLogger(Query.class);

    private String name;
    private QueryType type;
    private Schema schema;

    private List<SelectFunction> select;
    private List<Aggregator> aggregators;
    private Query from;
    private Filter.ConditionNode where;
    private List<String> groupBy;
    private Filter.ConditionNode having;

    public static class Properties {

        private String name;
        private JsonArray select;
        private JsonObject from;
        private JsonElement where;
        private List<String> groupBy;
        private JsonElement having;

    }

    public enum QueryType {
        table,
        subquery,
        with
    }

    public static List<Map<String, Object>> applySingle(
            final Query query,
            final Map<String, Object> inputPrimitiveValues,
            final Instant timestamp) {

        final Map<String, List<Map<String, Object>>> values = new HashMap<>();
        final List<Map<String, Object>> list = new ArrayList<>();
        list.add(inputPrimitiveValues);
        values.put("INPUT", list);

        return _apply(query, values, timestamp);
    }

    public static List<Map<String, Object>> applyMulti(
            final Query query,
            final List<Map<String, Object>> inputPrimitiveValues,
            final Instant timestamp) {

        final Map<String, List<Map<String, Object>>> values = new HashMap<>();
        values.put("INPUT", inputPrimitiveValues);

        return _apply(query, values, timestamp);
    }

    private static List<Map<String, Object>> _apply(
            final Query query,
            final Map<String, List<Map<String, Object>>> values,
            final Instant timestamp) {

        if(query.from != null) {
            final List<Map<String, Object>> fromValues = _apply(query.from, values, timestamp);
            values.put(query.from.name, fromValues);
        }

        if(query.where != null) {
            if(!Filter.filter(query.where, query.from.schema, null)) {
                return null;
            }
        }

        final List<Map<String, Object>> inputPrimitiveValues = values.get(query.from.name);

        if(query.groupBy != null) {
            return null;
        } else {
            final List<Map<String, Object>> results = new ArrayList<>();
            for(final Map<String, Object> map : inputPrimitiveValues) {
                final Map<String, Object> result = SelectFunction.apply(query.select, map, timestamp);
                results.add(result);
            }
            return results;
        }
    }

    public static Planner createPlanner(final MemorySchema schema) {
        final SchemaPlus rootSchema = Frameworks.createRootSchema(true);
        final SchemaPlus defaultSchema = rootSchema.add("DefaultSchema", schema);
        final SqlParser.Config insensitiveParser = SqlParser.configBuilder()
                .setCaseSensitive(false)
                .setLex(Lex.BIG_QUERY)
                .build();
        final FrameworkConfig config = Frameworks.newConfigBuilder()
                .parserConfig(insensitiveParser)
                .defaultSchema(defaultSchema)
                .build();
        return Frameworks.getPlanner(config);
    }

    public static Schema createQueryResultSchema(final List<String> inputNames, final List<Schema> inputSchemas, String sql) {
        final List<MemorySchema.MemoryTable> tables = new ArrayList<>();
        for(int i=0; i<inputNames.size(); i++) {
            final MemorySchema.MemoryTable table = MemorySchema
                    .createTable(inputNames.get(i), inputSchemas.get(i), new ArrayList<>());
            tables.add(table);
        }
        final MemorySchema memorySchema = MemorySchema.create("memorySchema", tables);
        return createQueryResultSchema(memorySchema, sql);
    }

    public static Schema createQueryResultSchema(final MemorySchema schema, String sql) {
        try(final Planner planner = createPlanner(schema);
            final PreparedStatement run = createStatement(planner, sql)) {

            final ResultSetMetaData resultSetMetadata = run.getMetaData();
            return CalciteSchemaUtil.convertSchema(resultSetMetadata);
        } catch (SqlParseException | ValidationException | RelConversionException | SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public static List<Map<String, Object>> execute(final Planner planner, String sql) {
        try(final PreparedStatement statement = createStatement(planner, sql)) {
            return execute(statement);
        } catch (SqlParseException | ValidationException | RelConversionException | SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public static List<Map<String, Object>> execute(final PreparedStatement statement) {
        try(final ResultSet resultSet = statement.executeQuery()) {
            return CalciteSchemaUtil.convert(resultSet);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public static PreparedStatement createStatement(final Planner planner, String sql) throws SqlParseException, ValidationException, RelConversionException {
        final SqlNode sqlNode = planner.parse(sql);

        // Validate the tree
        final SqlNode sqlNodeValidated = planner.validate(sqlNode);
        final RelRoot relRoot = planner.rel(sqlNodeValidated);
        final RelNode relNode = relRoot.project();

        final RelWriter relWriter = new RelWriterImpl(new PrintWriter(System.out), SqlExplainLevel.EXPPLAN_ATTRIBUTES, false);
        relNode.explain(relWriter);

        try {
            Connection connection = DriverManager.getConnection("jdbc:beam-vendor-calcite:");
            //Connection connection = DriverManager.getConnection("jdbc:calcite:");
            CalciteConnection c = connection.unwrap(CalciteConnection.class);
            RelRunner runner = c.unwrap(RelRunner.class);
            return runner.prepareStatement(relNode);
        } catch (Throwable e) {
            //RelRunners.run(relNode)
            throw new RuntimeException("Failed to parse sql: ", e);
        }
        //return RelRunners.run(relNode);
    }

    public static Transform of(
            final String jobName,
            final String name,
            final String sql,
            final List<String> inputNames,
            final List<Schema> inputSchemas,
            final boolean flatten,
            final boolean failFast) {

        return new Transform(jobName, name, sql, inputNames, inputSchemas, flatten, failFast);
    }


    public static class Transform extends PTransform<PCollection<MElement>, PCollectionTuple> {

        private final String jobName;
        private final String moduleName;
        private final String sql;
        private final List<String> inputNames;
        private final List<Schema> inputSchemas;
        private final Boolean flatten;
        private final Boolean failFast;

        public final Schema outputSchema;
        public final TupleTag<MElement> outputTag;
        public final TupleTag<MElement> failuresTag;


        Transform(
                final String jobName,
                final String moduleName,
                final String sql,
                final List<String> inputNames,
                final List<Schema> inputSchemas,
                final Boolean flatten,
                final Boolean failFast) {

            this.jobName = jobName;
            this.moduleName = moduleName;

            this.sql = sql;
            this.inputNames = inputNames;
            this.inputSchemas = inputSchemas;
            this.flatten = flatten;
            this.failFast = failFast;

            final Schema queryResultSchema = createQueryResultSchema(inputNames, inputSchemas, sql);
            final Schema.FieldType queryResultType = Schema.FieldType.element(queryResultSchema);

            if(flatten) {
                this.outputSchema = Schema.builder(queryResultSchema)
                        .build();
            } else {
                this.outputSchema = Schema.builder()
                        .withField("results", Schema.FieldType.array(queryResultType))
                        .build();
            }

            this.outputTag = new TupleTag<>() {};
            this.failuresTag = new TupleTag<>() {};
        }

        @Override
        public PCollectionTuple expand(PCollection<MElement> input) {
            final PCollection<MElement> output = input
                    .apply("Query", ParDo.of(new QueryDoFn(
                            jobName, moduleName, inputNames, inputSchemas, sql, failFast, flatten, failuresTag)))
                    .setCoder(ElementCoder.of(outputSchema));
            return PCollectionTuple
                    .of(outputTag, output);
        }

        private static class QueryDoFn extends DoFn<MElement, MElement> {

            private final String jobName;
            private final String moduleName;

            private final List<String> inputNames;
            private final List<Schema> inputSchemas;
            private final String sql;

            private final Boolean flatten;
            private final Boolean failFast;
            private final TupleTag<MElement> failuresTag;

            private transient List<List<MElement>> elementsList;

            private transient Planner planner;
            private transient PreparedStatement statement;

            QueryDoFn(
                    final String jobName,
                    final String moduleName,
                    final List<String> inputNames,
                    final List<Schema> inputSchemas,
                    final String sql,
                    final Boolean flatten,
                    final Boolean failFast,
                    final TupleTag<MElement> failuresTag) {

                this.jobName = jobName;
                this.moduleName = moduleName;
                this.inputNames = inputNames;
                this.inputSchemas = inputSchemas;
                this.sql = sql;
                this.flatten = flatten;
                this.failFast = failFast;
                this.failuresTag = failuresTag;
            }

            @Setup
            public void setup() throws Exception {
                this.elementsList = new ArrayList<>();
                final List<MemorySchema.MemoryTable> tables = new ArrayList<>();
                for(int i=0; i<inputNames.size(); i++) {
                    this.elementsList.add(new ArrayList<>());
                    final MemorySchema.MemoryTable table = MemorySchema.createTable(inputNames.get(i), inputSchemas.get(i), elementsList.get(i));
                    tables.add(table);
                }

                final MemorySchema memorySchema = MemorySchema.create("schema", tables);
                this.planner = createPlanner(memorySchema);
                this.statement = createStatement(planner, sql);
            }

            @ProcessElement
            public void processElement(final ProcessContext c) {
                final MElement element = c.element();
                if(element == null) {
                    return;
                }
                final int index = element.getIndex();
                this.elementsList.get(index).clear();
                this.elementsList.get(index).add(element);
                final String source = inputNames.get(index);

                try(final ResultSet resultSet = statement.executeQuery()) {
                    final List<Map<String, Object>> results = CalciteSchemaUtil.convert(resultSet);
                    if(flatten) {
                        for(final Map<String, Object> result : results) {
                            final MElement output = MElement.of(result, c.timestamp());
                            c.output(output);
                        }
                    } else {
                        final Map<String, Object> values = new HashMap<>();
                        values.put("results", results);
                        final MElement output = MElement.of(values, c.timestamp());
                        c.output(output);
                    }
                } catch (final Throwable e) {
                    String errorMessage = MFailure.convertThrowableMessage(e);
                    LOG.error("SQL query error: {}, {} for input: {} from: {}", e, errorMessage, element, source);
                    if(failFast) {
                        throw new IllegalStateException(errorMessage, e);
                    }
                    final MFailure failure = MFailure.of(jobName, moduleName, element.toString(), errorMessage, c.timestamp());
                    //if(outputFailure) {
                    //    c.output(failuresTag, failureElement.toElement(c.timestamp()));
                    //}
                    c.output(failuresTag, failure.toElement(c.timestamp()));
                }

            }


            @Teardown
            public void teardown() {
                if(statement != null) {
                    try {
                        statement.close();
                    } catch (SQLException e) {
                        LOG.error("Failed teardown");
                    }
                }
                if(planner != null) {
                    planner.close();
                }
            }

        }
    }
}
