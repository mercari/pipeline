package com.mercari.solution.util.pipeline;

import com.mercari.solution.module.MElement;
import com.mercari.solution.module.Schema;
import com.mercari.solution.util.schema.CalciteSchemaUtil;
import com.mercari.solution.util.sql.calcite.MemorySchema;

import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.config.Lex;
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
import java.io.Serializable;
import java.sql.*;
import java.util.*;

public class Query implements Serializable {

    private static final String DEFAULT_TABLE_NAME = "INPUT";
    private static final Logger LOG = LoggerFactory.getLogger(Query.class);

    private final String sql;

    private final Map<String, Schema> inputSchemas;
    private final Schema outputSchema;

    private transient Planner planner;
    private transient PreparedStatement statement;
    private transient Map<String,List<MElement>> elements;

    public Schema getOutputSchema() {
        return outputSchema;
    }

    Query(
            final Map<String, Schema> inputSchemas,
            final String sql) {

        this.inputSchemas = inputSchemas;
        this.outputSchema = createQueryResultSchema(inputSchemas, sql);
        this.sql = sql;
    }

    public static Query of(
            final String name,
            final List<Schema.Field> inputFields,
            final String sql) {

        final Map<String, Schema> inputSchemas = new HashMap<>();
        inputSchemas.put(name, Schema.of(inputFields));

        return of(inputSchemas, sql);
    }

    public static Query of(
            final String name,
            final Schema inputSchema,
            final String sql) {

        final Map<String, Schema> inputSchemas = new HashMap<>();
        inputSchemas.put(name, inputSchema);
        return of(inputSchemas, sql);
    }

    public static Query of(
            final Map<String, Schema> inputSchemas,
            final String sql) {

        return new Query(inputSchemas, sql);
    }


    public void setup() {
        final List<MemorySchema.MemoryTable> tables = new ArrayList<>();
        this.elements = new HashMap<>();
        for(final Map.Entry<String, Schema> entry : inputSchemas.entrySet()) {
            this.elements.put(entry.getKey(), new ArrayList<>());
            tables.add(MemorySchema.createTable(entry.getKey(), entry.getValue(), this.elements.get(entry.getKey())));
        }
        final MemorySchema memorySchema = MemorySchema.create("memorySchema", tables);
        this.planner = createPlanner(memorySchema);
        try {
            this.statement = createStatement(this.planner, sql);
        } catch (final Throwable e) {
            throw new IllegalArgumentException("failed to init query for query: " + sql, e);
        }
    }

    public void teardown() {
        try {
            if (this.statement != null && !this.statement.isClosed()) {
                this.statement.close();
            }
        } catch (final SQLException e) {
            LOG.error("failed to close statement: {}", statement);
        } finally {
            this.statement = null;
        }

        try {
            if(this.planner != null) {
                this.planner.close();
            }
        } catch (final Throwable e) {
            LOG.error("failed to close planner: {}", planner);
        } finally {
            this.planner = null;
        }
    }

    public List<MElement> execute(final MElement input, final Instant timestamp) {
        return execute(List.of(input), timestamp);
    }

    public List<MElement> execute(final List<MElement> inputs, final Instant timestamp) {
        return execute(Map.of(DEFAULT_TABLE_NAME, inputs), timestamp);
    }

    public List<MElement> execute_(final Map<String, MElement> inputs, final Instant timestamp) {
        final Map<String, List<MElement>> elements = new HashMap<>();
        for(final Map.Entry<String, MElement> entry : inputs.entrySet()) {
            elements.put(entry.getKey(), List.of(entry.getValue()));
        }
        return execute(elements, timestamp);
    }

    public List<MElement> execute(final Map<String, List<MElement>> inputs, final Instant timestamp) {
        for(final Map.Entry<String, List<MElement>> entry : inputs.entrySet()) {
            if(inputs.containsKey(entry.getKey())) {
                elements.get(entry.getKey()).clear();
                elements.get(entry.getKey()).addAll(entry.getValue());
            }
        }
        final List<Map<String, Object>> valuesList = execute(statement);
        return MElement.ofList(valuesList, timestamp);
    }

    public static Planner createPlanner(final MemorySchema schema) {

        final SchemaPlus rootSchema = Frameworks.createRootSchema(true);
        final SchemaPlus defaultSchema = rootSchema.add("DefaultSchema", schema);
        final SqlParser.Config insensitiveParser = SqlParser.configBuilder()
                .setCaseSensitive(false)
                .setLex(Lex.BIG_QUERY)
                .build();

        /*
        SqlOperatorTable customOpTable = new ListSqlOperatorTable(List.of(BigtableFunctions.create()));
        SqlOperatorTable operatorTable = SqlOperatorTables.chain(
                SqlStdOperatorTable.instance(),
                customOpTable
        );
        SqlStdOperatorTable sqlStdOperatorTable = SqlStdOperatorTable.instance();
        sqlStdOperatorTable.register(BigtableFunctions.create());
         */


        final FrameworkConfig config = Frameworks.newConfigBuilder()
                .parserConfig(insensitiveParser)
                .defaultSchema(defaultSchema)
                //.operatorTable(sqlStdOperatorTable)
                .build();
        return Frameworks.getPlanner(config);
    }

    public static Schema createQueryResultSchema(
            final String name,
            final Schema inputSchema,
            final String sql) {

        return createQueryResultSchema(List.of(name), List.of(inputSchema), sql);
    }

    public static Schema createQueryResultSchema(
            final List<String> inputNames,
            final List<Schema> inputSchemas,
            final String sql) {

        final List<MemorySchema.MemoryTable> tables = new ArrayList<>();
        for(int i=0; i<inputNames.size(); i++) {
            final MemorySchema.MemoryTable table = MemorySchema
                    .createTable(inputNames.get(i), inputSchemas.get(i), new ArrayList<>());
            tables.add(table);
        }
        final MemorySchema memorySchema = MemorySchema.create("memorySchema", tables);
        return createQueryResultSchema(memorySchema, sql);
    }

    public static Schema createQueryResultSchema(
            final Map<String,Schema> inputSchemas,
            final String sql) {

        final List<MemorySchema.MemoryTable> tables = new ArrayList<>();
        for(final Map.Entry<String,Schema> entry : inputSchemas.entrySet()) {
            final MemorySchema.MemoryTable table = MemorySchema
                    .createTable(entry.getKey(), entry.getValue(), new ArrayList<>());
            tables.add(table);
        }
        final MemorySchema memorySchema = MemorySchema.create("memorySchema", tables);
        return createQueryResultSchema(memorySchema, sql);
    }

    public static Schema createQueryResultSchema(
            final MemorySchema schema,
            final String sql) {

        try(final Planner planner = createPlanner(schema);
            final PreparedStatement run = createStatement(planner, sql)) {

            final ResultSetMetaData resultSetMetadata = run.getMetaData();
            return CalciteSchemaUtil.convertSchema(resultSetMetadata);
        } catch (SqlParseException | ValidationException | RelConversionException | SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public static List<Map<String, Object>> execute(
            final Planner planner,
            final String sql) {

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

    public static PreparedStatement createStatement(
            final Planner planner,
            final String sql) throws SqlParseException, ValidationException, RelConversionException {

        final SqlNode sqlNode = planner.parse(sql);

        // Validate the tree
        final SqlNode sqlNodeValidated = planner.validate(sqlNode);
        final RelRoot relRoot = planner.rel(sqlNodeValidated);
        final RelNode relNode = relRoot.project();

        //final Pair<SqlNode, RelDataType> a = planner.validateAndGetType(sqlNodeValidated);
        //final Schema s = CalciteSchemaUtil.convertSchema(relNode.getRowType());

        final RelWriter relWriter = new RelWriterImpl(new PrintWriter(System.out), SqlExplainLevel.EXPPLAN_ATTRIBUTES, false);
        relNode.explain(relWriter);

        return RelRunners.run(relNode);
    }

}
