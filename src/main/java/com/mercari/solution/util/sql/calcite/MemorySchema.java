package com.mercari.solution.util.sql.calcite;

import com.mercari.solution.module.MElement;
import com.mercari.solution.module.Schema;
import com.mercari.solution.util.schema.CalciteSchemaUtil;
import org.apache.beam.vendor.calcite.v1_28_0.com.google.common.collect.ImmutableMultimap;
import org.apache.beam.vendor.calcite.v1_28_0.com.google.common.collect.Multimap;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.DataContext;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.linq4j.Enumerable;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.linq4j.Linq4j;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.rel.type.RelDataType;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.schema.*;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.schema.impl.AbstractTable;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.sql.*;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.sql.type.OperandTypes;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.sql.type.ReturnTypes;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.sql.validate.SqlValidator;

import java.io.Serializable;
import java.sql.Date;
import java.time.LocalDate;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MemorySchema extends AbstractSchema implements Serializable {

    private final String name;
    private final Map<String, Table> tableMap;

    public MemorySchema(String schemaName, Map<String, Table> tableMap) {
        this.name = schemaName;
        this.tableMap = tableMap;
    }

    public static MemorySchema create(String name, List<MemoryTable> tables) {
        final Map<String, Table> tableMap = new HashMap<>();
        for(final MemoryTable table : tables) {
            tableMap.put(table.name, table);
        }
        return new MemorySchema(name, tableMap);
    }

    public static MemoryTable createTable(String name, Schema schema, List<MElement> elements) {
        return new MemoryTable(name, schema, elements);
    }

    @Override
    public Map<String, Table> getTableMap() {
        return tableMap;
    }

    @Override
    protected Multimap<String, Function> getFunctionMultimap() {
        return ImmutableMultimap.<String, Function>builder()
                // example define UDFs
                //.put("ExampleStringLengthFunctionEval", ScalarFunctionImpl.create(Types.lookupMethod(ExampleStringLengthFunction.class, "eval", String.class)))
                //.put("ExampleStringLengthFunctionCurrentDate", ScalarFunctionImpl.create(Types.lookupMethod(ExampleStringLengthFunction.class, "currentDate")))
                //.put("ExampleStructFunction", ScalarFunctionImpl.create(Types.lookupMethod(ExampleStructFunction.class, "eval", String.class)))
                //.put("ExampleAggregationFunction", AggregateFunctionImpl.create(ExampleAggregationFunction.class))
                //.put("", SqlBasicAggFunction.create("ARG_MAX", SqlKind.valueOf("ARG_MAX"), ReturnTypes.ARG0_NULLABLE_IF_EMPTY, OperandTypes.ANY_NUMERIC).withGroupOrder(Optionality.FORBIDDEN).withFunctionType(SqlFunctionCategory.SYSTEM))
                .build();
    }

    public static class MemoryTable extends AbstractTable implements ScannableTable, Serializable {

        private final String name;
        private final com.mercari.solution.module.Schema schema;
        private final MemoryStatistic statistic;
        private final Enumerable<MElement> enumerable;

        private RelDataType rowType;

        public MemoryTable(
                final String tableName,
                final com.mercari.solution.module.Schema schema,
                final List<MElement> elements) {

            this.name = tableName;
            this.schema = schema;
            this.statistic = new MemoryStatistic(elements.size());
            this.enumerable = Linq4j.asEnumerable(elements);
        }

        @Override
        public RelDataType getRowType(RelDataTypeFactory relDataTypeFactory) {
            if(this.rowType == null) {
                this.rowType = CalciteSchemaUtil.convertSchema(this.schema, relDataTypeFactory);
            }
            return this.rowType;
        }

        @Override
        public Enumerable<Object[]> scan(DataContext dataContext) {
            return this.enumerable.select(CalciteSchemaUtil.createFieldSelector(schema));
        }

        public static class MemoryStatistic implements Statistic {

            private final Long count;

            public MemoryStatistic(long count) {
                this.count = count;
            }

            @Override
            public Double getRowCount() {
                return count.doubleValue();
            }

        }

    }

    public static class ExampleFunction extends SqlFunction {

        public ExampleFunction() {
            super("MY_ADD", SqlKind.PLUS, ReturnTypes.INTEGER, null,
                    OperandTypes.NUMERIC_NUMERIC, SqlFunctionCategory.NUMERIC);
        }

        @Override
        public SqlNode rewriteCall(SqlValidator validator, SqlCall call) {
            return super.rewriteCall(validator, call);
        }

    }

    public static class ExampleStringLengthFunction {

        public static Integer eval(String ok) {
            return ok.length();
        }

        public static Date currentDate() {
            return Date.valueOf(LocalDate.now());
        }

    }

    public static class ExampleStructFunction {

        public static Object eval(Object names) {
            return switch (names) {
                case Object[] objects -> {
                    final MElement.Builder builder = MElement.builder();
                    for(int i=0; i<objects.length; i+=2) {
                        final String fieldName = objects[i].toString();
                        final Object fieldValue = objects[i+1];
                        final Object fieldValue_ = CalciteSchemaUtil.convertPrimitiveValue(fieldValue);
                        builder.withPrimitiveValue(fieldName, fieldValue_);
                    }
                    yield builder.build();
                }
                default -> throw new IllegalArgumentException();
            };
        }

    }

    public static class ExampleAggregationFunction {

        public double init() {
            return 0L;
        }

        public double add(double accumulator, int v) {
            return accumulator + v;
        }

        public double merge(double accumulator0, double accumulator1) {
            return accumulator0 + accumulator1;
        }

        public double result(double accumulator) {
            return accumulator;
        }

    }

}
