package com.mercari.solution.util.pipeline.aggregation;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.mercari.solution.module.MElement;
import com.mercari.solution.module.Schema;
import com.mercari.solution.util.pipeline.Filter;
import com.mercari.solution.util.ExpressionUtil;
import net.objecthunter.exp4j.Expression;
import org.joda.time.Instant;

import java.util.*;

public class SimpleRegression implements AggregateFunction {

    private List<Schema.Field> inputFields;
    private Schema.FieldType outputFieldType;

    private String name;
    private String field;
    private String xField;
    private String expression;
    private String weightField;
    private String weightExpression;
    private Boolean hasIntercept;
    private String condition;

    private List<Range> ranges;

    private Boolean ignore;

    /*
    private String outputSlopeName;
    private String outputInterceptName;
    private String outputWeightName;
    private String outputSumSquaredErrorsName;
    private String outputMeanSquaredErrorsName;
    private String outputRootMeanSquaredErrorsName;
    private String outputTotalSumSquaresName;
    private String outputXSumSquaresName;
    private String outputSumOfCrossProductsName;
    private String outputRegressionSumSquaresName;
    private String outputMeanSquareErrorName;
     */

    //
    private String accumKeyCountName;
    private String accumKeyWeightName;
    private String accumKeySumXName;
    private String accumKeySumXXName;
    private String accumKeySumYName;
    private String accumKeySumYYName;
    private String accumKeySumXYName;
    private String accumKeyXBarName;
    private String accumKeyYBarName;


    private transient Expression exp;
    private transient Set<String> variables;

    private transient Expression weightExp;
    private transient Set<String> weightVariables;

    private transient Filter.ConditionNode conditionNode;


    public static SimpleRegression of(
            final String name,
            final List<Schema.Field> inputFields,
            final String field,
            final String expression,
            final String condition,
            final List<Range> ranges,
            final Boolean ignore,
            final JsonObject params) {

        final SimpleRegression regression = new SimpleRegression();
        regression.name = name;
        regression.field = field;
        regression.expression = expression;
        regression.condition = condition;
        regression.ranges = ranges;
        regression.ignore = ignore;

        regression.accumKeyCountName = name + ".count";
        regression.accumKeyWeightName = name + ".weight";
        regression.accumKeySumXName = name + ".sumX";
        regression.accumKeySumXXName = name + ".sumXX";
        regression.accumKeySumYName = name + ".sumY";
        regression.accumKeySumYYName = name + ".sumYY";
        regression.accumKeySumXYName = name + ".sumXY";
        regression.accumKeyXBarName = name + ".xBar";
        regression.accumKeyYBarName = name + ".yBar";

        /*
        regression.outputSlopeName = regression.outputFieldName("Slope");
        regression.outputInterceptName = regression.outputFieldName("Intercept");
        regression.outputWeightName = regression.outputFieldName("N");
        regression.outputSumSquaredErrorsName = regression.outputFieldName("SSE");
        regression.outputMeanSquaredErrorsName = regression.outputFieldName("MSE");
        regression.outputRootMeanSquaredErrorsName = regression.outputFieldName("RMSE");
         */

        regression.inputFields = new ArrayList<>();

        if(field != null) {
            regression.inputFields.add(Schema.Field.of(field, Schema.getField(inputFields, field).getFieldType()));
        } else {
            for(final String variable : ExpressionUtil.estimateVariables(expression)) {
                regression.inputFields.add(Schema.Field.of(variable, Schema.getField(inputFields, variable).getFieldType()));
            }
        }

        if(params.has("weightField")) {
            regression.weightField = params.get("weightField").getAsString();
            regression.inputFields.add(Schema.Field.of(regression.weightField, Schema.getField(inputFields, regression.weightField).getFieldType()));
        } else if(params.has("weightExpression")) {
            regression.weightExpression = params.get("weightExpression").getAsString();
            for(final String variable : ExpressionUtil.estimateVariables(regression.weightExpression)) {
                regression.inputFields.add(Schema.Field.of(variable, Schema.getField(inputFields, variable).getFieldType()));
            }
        }

        regression.outputFieldType = Schema.FieldType.element(List.of(
                Schema.Field.of("Slope", Schema.FieldType.FLOAT64.withNullable(true)),
                Schema.Field.of("Intercept", Schema.FieldType.FLOAT64.withNullable(true)),
                Schema.Field.of("RMSE", Schema.FieldType.FLOAT64.withNullable(true)),
                Schema.Field.of("N", Schema.FieldType.INT64.withNullable(true))
        ));

        if(params.has("xField")) {
            regression.xField = params.get("xField").getAsString();
            regression.inputFields.add(Schema.Field.of(regression.xField, Schema.getField(inputFields, regression.xField).getFieldType()));
        } else {
            regression.xField = null;
        }

        if(params.has("hasIntercept")) {
            regression.hasIntercept = params.get("hasIntercept").getAsBoolean();
        } else {
            regression.hasIntercept = true;
        }

        return regression;
    }

    @Override
    public String getName() {
        return this.name;
    }

    @Override
    public boolean ignore() {
        return Optional.ofNullable(this.ignore).orElse(false);
    }

    @Override
    public Boolean filter(final MElement element) {
        return AggregateFunction.filter(conditionNode, element);
    }

    @Override
    public List<Range> getRanges() {
        return ranges;
    }

    @Override
    public List<String> validate(int parent, int index) {
        final List<String> errorMessages = new ArrayList<>();

        return errorMessages;
    }

    @Override
    public void setup() {
        if(this.expression != null) {
            final Set<String> variables = ExpressionUtil.estimateVariables(this.expression);
            this.variables = variables;
            this.exp = ExpressionUtil.createDefaultExpression(this.expression, variables);
        }
        if(this.weightExpression != null) {
            final Set<String> weightVariables = ExpressionUtil.estimateVariables(this.weightExpression);
            this.weightVariables = weightVariables;
            this.weightExp = ExpressionUtil.createDefaultExpression(this.weightExpression, weightVariables);
        }
        if(this.condition != null) {
            this.conditionNode = Filter.parse(new Gson().fromJson(this.condition, JsonElement.class));
        }
    }

    @Override
    public Object apply(Map<String, Object> input, Instant timestamp) {
        return null;
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
    public Accumulator addInput(final Accumulator accumulator, final MElement input, final Integer co, final Instant timestamp) {
        final Double y;
        if(field != null) {
            y = input.getAsDouble(field);
        } else {
            y = AggregateFunction.eval(this.exp, variables, input);
        }
        final Double x;
        if(xField != null) {
            x = input.getAsDouble(xField);
        } else {
            x = Long.valueOf(input.getEpochMillis()).doubleValue();
        }

        if(x == null || y == null) {
            return accumulator;
        }

        final double inputWeight;
        if(weightField != null) {
            inputWeight = Optional.ofNullable(input.getAsDouble(weightField)).orElse(0D);
        } else if(weightExpression != null) {
            inputWeight = Optional.ofNullable(AggregateFunction.eval(this.weightExp, weightVariables, input)).orElse(0D);
        } else {
            inputWeight = 1D;
        }

        final double count = getDouble(accumulator, accumKeyCountName, 0D);
        final double weight = getDouble(accumulator, accumKeyWeightName, 0D);
        double xBar = getDouble(accumulator, accumKeyXBarName, 0D);
        double yBar = getDouble(accumulator, accumKeyYBarName, 0D);
        double sumX = getDouble(accumulator, accumKeySumXName, 0D);
        double sumY = getDouble(accumulator, accumKeySumYName, 0D);
        double sumXX = getDouble(accumulator, accumKeySumXXName, 0D);
        double sumYY = getDouble(accumulator, accumKeySumYYName, 0D);
        double sumXY = getDouble(accumulator, accumKeySumXYName, 0D);

        if(weight == 0) {
            xBar = x;
            yBar = y;
        } else {
            if(hasIntercept) {
                final double fact1 = inputWeight + weight;
                final double fact2 = weight / fact1;
                final double dx = x - xBar;
                final double dy = y - yBar;
                sumXX += (dx * dx * fact2);
                sumYY += (dy * dy * fact2);
                sumXY += (dx * dy * fact2);
                xBar += (dx / fact1);
                yBar += (dy / fact1);
            }
        }
        if(!hasIntercept) {
            sumXX += (x * x);
            sumYY += (y * y);
            sumXY += (x * y);
        }
        sumX += x;
        sumY += y;

        accumulator.put(accumKeyCountName, count + 1);
        accumulator.put(accumKeyWeightName, weight + inputWeight);
        accumulator.put(accumKeyXBarName, xBar);
        accumulator.put(accumKeyYBarName, yBar);
        accumulator.put(accumKeySumXName, sumX);
        accumulator.put(accumKeySumYName, sumY);
        accumulator.put(accumKeySumXXName, sumXX);
        accumulator.put(accumKeySumYYName, sumYY);
        accumulator.put(accumKeySumXYName, sumXY);

        return accumulator;
    }

    @Override
    public Accumulator addInput(final Accumulator accumulator, final MElement input) {
        return addInput(accumulator, input, null, null);
    }

    @Override
    public Accumulator mergeAccumulator(final Accumulator base, final Accumulator input) {
        final Double baseWeight = getDouble(base, accumKeyWeightName, 0D);
        final Double inputWeight = getDouble(input, accumKeyWeightName, 0D);
        if(inputWeight == 0) {
            return base;
        } else if(baseWeight == 0) {
            base.put(accumKeyXBarName, Optional.ofNullable(input.getAsDouble(accumKeyXBarName)).orElse(0D));
            base.put(accumKeyYBarName, Optional.ofNullable(input.getAsDouble(accumKeyYBarName)).orElse(0D));
            base.put(accumKeySumXXName, Optional.ofNullable(input.getAsDouble(accumKeySumXXName)).orElse(0D));
            base.put(accumKeySumYYName, Optional.ofNullable(input.getAsDouble(accumKeySumYYName)).orElse(0D));
            base.put(accumKeySumXYName, Optional.ofNullable(input.getAsDouble(accumKeySumXYName)).orElse(0D));
        } else {
            if(hasIntercept) {
                final double fact1 = inputWeight / (baseWeight + inputWeight);
                final double fact2 = baseWeight * inputWeight / (baseWeight + inputWeight);
                final double dx = getDouble(input, accumKeyXBarName,0D) - getDouble(base, accumKeyXBarName, 0D);
                final double dy = getDouble(input, accumKeyYBarName,0D) - getDouble(base, accumKeyYBarName, 0D);
                final double sumXX = getDouble(base, accumKeySumXXName, 0D) + (getDouble(input, accumKeySumXXName, 0D) + dx * dx * fact2);
                final double sumYY = getDouble(base, accumKeySumYYName, 0D) + (getDouble(input, accumKeySumYYName, 0D) + dy * dy * fact2);
                final double sumXY = getDouble(base, accumKeySumXYName, 0D) + (getDouble(input, accumKeySumXYName, 0D) + dx * dy * fact2);
                final double xBar = getDouble(base, accumKeyXBarName, 0D) + dx * fact1;
                final double yBar = getDouble(base, accumKeyYBarName, 0D) + dy * fact1;
                base.put(accumKeySumXXName, sumXX);
                base.put(accumKeySumYYName, sumYY);
                base.put(accumKeySumXYName, sumXY);
                base.put(accumKeyXBarName, xBar);
                base.put(accumKeyYBarName, yBar);
            } else {
                final double sumXX = getDouble(base, accumKeySumXXName, 0D) + getDouble(input, accumKeySumXXName, 0D);
                final double sumYY = getDouble(base, accumKeySumYYName, 0D) + getDouble(input, accumKeySumYYName, 0D);
                final double sumXY = getDouble(base, accumKeySumXYName, 0D) + getDouble(input, accumKeySumXYName, 0D);
                base.put(accumKeySumXXName, sumXX);
                base.put(accumKeySumYYName, sumYY);
                base.put(accumKeySumXYName, sumXY);
            }
        }

        final double sumX = getDouble(base, accumKeySumXName, 0D) + getDouble(input, accumKeySumXName, 0D);
        final double sumY = getDouble(base, accumKeySumYName, 0D) + getDouble(input, accumKeySumYName, 0D);
        base.put(accumKeySumXName, sumX);
        base.put(accumKeySumYName, sumY);
        base.put(accumKeyWeightName, baseWeight + inputWeight);
        base.put(accumKeyCountName, getDouble(base, accumKeyCountName, 0D) + getDouble(input, accumKeyCountName, 0D));

        return base;
    }

    @Override
    public Object extractOutput(final Accumulator accumulator,
                                            final Map<String, Object> values) {

        final Map<String, Object> output = new HashMap<>();
        final double slope = getSlope(accumulator);
        output.put("Slope", slope);
        output.put("Intercept", getIntercept(accumulator, slope));
        output.put("RMSE", getRootMeanSumSquaredErrors(accumulator));
        output.put("N", getDouble(accumulator, accumKeyCountName, 0D));
        return output;
    }

    private double getSlope(final Accumulator accumulator) {
        final double count = getDouble(accumulator, accumKeyCountName, 0D);
        if(count < 2D) {
            return Double.NaN;
        }
        final double sumXX = getDouble(accumulator, accumKeySumXXName, 0D);
        if(Math.abs(sumXX) < 10 * Double.MIN_VALUE) {
            return Double.NaN;
        }
        final double sumXY = getDouble(accumulator, accumKeySumXYName, 0D);
        return sumXY / sumXX;
    }

    private double getIntercept(final Accumulator accumulator, final double slope) {
        if(hasIntercept) {
            final double weight = getDouble(accumulator, accumKeyWeightName, 0D);
            if(weight == 0) {
                return 0.0D;
            }
            final double sumX = getDouble(accumulator, accumKeySumXName, 0D);
            final double sumY = getDouble(accumulator, accumKeySumYName, 0D);
            return (sumY - slope * sumX) / weight;
        }
        return 0.0D;
    }

    private double getWeight(final Accumulator accumulator) {
        final double count = getDouble(accumulator, accumKeyCountName, 0D);
        if(count < 2D) {
            return Double.NaN;
        }
        return getDouble(accumulator, accumKeyWeightName, 0D);
    }

    private double getSumSquaredErrors(final Accumulator accumulator) {
        final double sumXX = getDouble(accumulator, accumKeySumXXName, 0D);
        final double sumYY = getDouble(accumulator, accumKeySumYYName, 0D);
        final double sumXY = getDouble(accumulator, accumKeySumXYName, 0D);
        if(sumXX == 0) {
            return Double.NaN;
        }
        return Math.max(0D, sumYY - sumXY * sumXY / sumXX);
    }

    private double getMeanSumSquaredErrors(final Accumulator accumulator) {
        final double weight = getDouble(accumulator, accumKeyWeightName, 0D);
        if(weight == 0) {
            return Double.NaN;
        }
        double sumSquaredErrors = getSumSquaredErrors(accumulator);
        if(Double.isNaN(sumSquaredErrors)) {
            return Double.NaN;
        }
        return sumSquaredErrors / weight;
    }

    private double getRootMeanSumSquaredErrors(final Accumulator accumulator) {
        double meanSumSquaredErrors = getMeanSumSquaredErrors(accumulator);
        if(Double.isNaN(meanSumSquaredErrors)) {
            return Double.NaN;
        }
        return Math.sqrt(meanSumSquaredErrors);
    }

    private static double getDouble(final Accumulator input, final String keyName, final Double defaultValue) {
        return Optional.ofNullable(input.getAsDouble(keyName)).orElse(defaultValue);
    }

}
