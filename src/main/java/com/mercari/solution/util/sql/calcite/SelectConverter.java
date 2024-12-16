package com.mercari.solution.util.sql.calcite;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.mercari.solution.module.Schema;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.sql.*;

import java.util.Map;

public class SelectConverter {

    public static JsonElement convert(final SqlNode node, final Map<String, Schema.Field> fields) {
        return switch (node) {
            case SqlIdentifier id -> {
                final JsonObject jsonObject = new JsonObject();
                jsonObject.addProperty("name", id.getSimple());
                System.out.println("id " + id.isStar());
                yield jsonObject;
            }
            case SqlLiteral literal -> {
                final JsonObject jsonObject = new JsonObject();
                final Schema.Type type = Schema.Type.of(literal.getTypeName().name());
                jsonObject.addProperty("type", type.name());
                System.out.println("literal: " + type + " value: " + literal.toValue());
                switch (type) {
                    case bool -> jsonObject.addProperty("value", literal.booleanValue());
                    case string -> jsonObject.addProperty("value", literal.toValue());
                    case int8, int16, int32 -> jsonObject.addProperty("value", literal.intValue(true));
                    case int64 -> jsonObject.addProperty("value", literal.longValue(true));
                    case decimal -> jsonObject.addProperty("value", literal.bigDecimalValue());
                    default -> {
                        switch (literal.getValue()) {
                            case String s -> jsonObject.addProperty("value", s);
                            case Number n -> jsonObject.addProperty("value", n);
                            case null -> jsonObject.add("value", JsonNull.INSTANCE);
                            default -> jsonObject.addProperty("value", literal.toValue());
                        }
                    }
                }
                yield jsonObject;
            }
            case SqlCall call -> convert(call, fields);
            case SqlNodeList list -> {
                throw new IllegalArgumentException();
            }
            case SqlIntervalQualifier interval -> {
                throw new IllegalArgumentException();
            }
            case SqlDataTypeSpec spec -> {
                final JsonObject jsonObject = new JsonObject();
                final String type = spec.getTypeName().getSimple();
                jsonObject.addProperty("type", Schema.Type.of(type).name());
                yield jsonObject;
            }
            case SqlDynamicParam param -> {
                throw new IllegalArgumentException();
            }
            default -> throw new IllegalArgumentException();
        };
    }

    public static JsonElement convert(final SqlCall call, final Map<String, Schema.Field> fields) {
        return switch (call) {
            case SqlSelect select -> {
                final JsonArray selectFunctions = new JsonArray();
                for(final SqlNode node : select.getSelectList()) {
                    if(node == null) {
                        continue;
                    }
                    final JsonElement selectFunction = convert(node, fields);
                    selectFunctions.add(selectFunction);
                }

                final JsonElement from = convert(select.getFrom(), fields);

                final JsonObject jsonObject = new JsonObject();
                jsonObject.add("from", from);
                jsonObject.add("select", selectFunctions);
                if(select.hasWhere()) {
                    final JsonElement where = convert(select.getWhere(), fields);
                    jsonObject.add("where", where);
                } else {
                    System.out.println("no where: " + select);
                }
                System.out.println("aaa: " + jsonObject);
                yield jsonObject;
            }
            case SqlBasicCall basic -> switch (basic.getKind()) {
                case AS -> {
                    if(basic.getOperandList().size() != 2) {
                        throw new IllegalArgumentException("Illegal AS operand size " + basic.getOperandList().size() + ", operands: " + basic.getOperandList());
                    }
                    final SqlNode first = basic.getOperandList().getFirst();
                    final SqlNode last = basic.getOperandList().getLast();
                    if(!(last instanceof SqlIdentifier)) {
                        throw new IllegalArgumentException();
                    }

                    final JsonElement left = convert(first, fields);
                    final JsonElement right = convert(last, fields);

                    final JsonObject jsonObject = new JsonObject();
                    if(left.isJsonObject()) {
                        for(final Map.Entry<String, JsonElement> entry : left.getAsJsonObject().entrySet()) {
                            System.out.println(entry.getValue());
                            jsonObject.add(entry.getKey(), entry.getValue());
                        }
                    } else {

                    }
                    jsonObject.addProperty("name", right.getAsJsonObject().get("name").getAsString());
                    yield jsonObject;

                }
                case CAST -> {
                    if(basic.getOperandList().size() != 2) {
                        throw new IllegalArgumentException("Illegal CAST operand size " + basic.getOperandList().size() + ", operands: " + basic.getOperandList());
                    }
                    final SqlNode first = basic.getOperandList().getFirst();
                    final SqlNode last = basic.getOperandList().getLast();

                    final JsonElement left = convert(first, fields);
                    final JsonElement right = convert(last, fields);
                    if(!left.isJsonObject() || !right.isJsonObject()) {
                        throw new IllegalArgumentException();
                    }
                    final String type = right.getAsJsonObject().get("type").getAsString();
                    if(Schema.Type.element.name().equals(type)) {
                        yield JsonNull.INSTANCE;
                    }

                    final JsonObject jsonObject = new JsonObject();
                    jsonObject.addProperty("func", "cast");
                    if(left.getAsJsonObject().has("name")) {
                        final String field = left.getAsJsonObject().get("name").getAsString();
                        jsonObject.addProperty("name", field);
                        jsonObject.addProperty("field", field);
                    } else if(left.getAsJsonObject().has("expression")) {
                        final String expression = left.getAsJsonObject().get("expression").getAsString();
                        jsonObject.addProperty("expression", expression);
                    }
                    jsonObject.addProperty("type", type);
                    yield jsonObject;
                }
                case ROW -> {
                    final JsonObject rowObject = new JsonObject();
                    final JsonArray fieldsArray = new JsonArray();
                    for(final SqlNode op : basic.getOperandList()) {
                        final JsonObject jsonObject = new JsonObject();
                        jsonObject.addProperty("field", ((SqlIdentifier)op).getSimple());
                        fieldsArray.add(jsonObject);
                    }
                    rowObject.add("fields", fieldsArray);
                    yield rowObject;
                }
                case ARRAY_VALUE_CONSTRUCTOR -> {
                    final JsonArray array = new JsonArray();
                    for(final SqlNode node : basic.getOperandList()) {
                        final JsonElement e = convert(node, fields);
                        array.add(e);
                    }
                    final JsonObject jsonObject = new JsonObject();
                    jsonObject.add("value", array);
                    yield jsonObject;
                }
                case TIMES, PLUS, MINUS, DIVIDE -> {
                    final JsonObject jsonObject = new JsonObject();
                    jsonObject.addProperty("expression", basic.toString());
                    yield jsonObject;
                }
                case AND, OR, NOT -> {
                    System.out.println(basic.getOperandList());
                    throw new IllegalArgumentException();
                }
                case OTHER_FUNCTION -> {
                    switch (basic.getOperator().getName()) {
                        case "FORMAT" -> System.out.println("format");
                    }
                    System.out.println("func kind: " + basic.getOperator().getName() + " od: " + basic.getOperandList());
                    yield JsonNull.INSTANCE;
                }
                case GREATER_THAN, GREATER_THAN_OR_EQUAL,
                     LESS_THAN, LESS_THAN_OR_EQUAL,
                     NOT_EQUALS, EQUALS -> {

                    System.out.println(basic.getOperator() + " : " + basic.getOperandList());

                    yield JsonNull.INSTANCE;
                }
                case UNNEST -> {
                    System.out.println("unn: " + basic.getOperator() + " : " + basic.getOperandList());
                    yield JsonNull.INSTANCE;
                }
                default -> throw new IllegalArgumentException("Not supported kind: " + basic.getKind());
            };
            case SqlWith with -> {
                final JsonArray items = new JsonArray();
                for(final SqlNode n : with.withList) {
                    final JsonElement withItem = convert(n, fields);
                    items.add(withItem);
                }
                final JsonElement body = convert(with.body, fields);
                if(!body.isJsonObject()) {
                    throw new IllegalArgumentException();
                }

                final JsonObject jsonObject = body.getAsJsonObject();

                yield jsonObject;
            }
            case SqlWithItem withItem -> {
                final JsonElement n = convert(withItem.query, fields);
                System.out.println("wihtItem: " + n);
                yield JsonNull.INSTANCE;
            }
            case SqlJoin join -> {
                System.out.println("join: " + join.getJoinType() + ", " + join.getRight());
                yield JsonNull.INSTANCE;
            }

            /*
            case SqlCase c -> System.out.println("case: " + c);
            case SqlJoin join -> System.out.println("join: " + join);
            case SqlMerge merge -> System.out.println("merge: " + merge);
            case SqlWindow window -> System.out.println("window: " + window);
            case SqlMatchRecognize match -> System.out.println("match: " + match);
            case SqlOrderBy order -> System.out.println("order: " + order);
            case SqlAlter alter -> System.out.println("alter: " + alter);
            case SqlDdl ddl -> System.out.println("ddl: " + ddl);
            case SqlDelete delete -> System.out.println("delete: " + delete);
            case SqlDescribeSchema describe -> System.out.println("describe: " + describe);
            case SqlDescribeTable describe -> System.out.println("describe: " + describe);
            case SqlExplain explain -> System.out.println("explain: " + explain);
            case SqlInsert insert -> System.out.println("insert: " + insert);
            case SqlSnapshot snapshot -> System.out.println("snapshot: " + snapshot);
            case SqlUpdate update -> System.out.println("update: " + update);
             */
            default -> throw new IllegalArgumentException("Not supported call: " + call.getKind());
        };
    }

}
