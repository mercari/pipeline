package com.mercari.solution.util.gcp;

import com.google.bigtable.v2.ColumnRange;
import com.google.bigtable.v2.RowFilter;
import com.google.bigtable.v2.TimestampRange;
import com.google.bigtable.v2.ValueRange;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.protobuf.ByteString;
import com.mercari.solution.util.DateTimeUtil;
import org.apache.beam.sdk.io.range.ByteKey;
import org.apache.beam.sdk.io.range.ByteKeyRange;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class BigtableUtil {

    public enum FilterType {
        // row-selection
        sample,
        row_key_regex,
        // cell-selection
        limit_cells_per_row,
        limit_cells_per_column,
        offset_cells_per_row,
        family_name_regex,
        column_qualifier_regex,
        column_range,
        value_range,
        value_regex,
        timestamp_range,
        // advanced-single-filters
        block,
        pass,
        sink,
        // modifying-filters
        label,
        strip,
        // composing-filters
        chain,
        interleave,
        condition

    }

    public static List<ByteKeyRange> createKeyRanges(final JsonElement jsonElement) {
        if(jsonElement == null || jsonElement.isJsonNull() || jsonElement.isJsonPrimitive()) {
            return Arrays.asList(ByteKeyRange.ALL_KEYS);
        }

        if(jsonElement.isJsonObject()) {
            final JsonObject jsonObject = jsonElement.getAsJsonObject();
            final ByteKey start;
            if(jsonObject.has("start")) {
                final String startKey = jsonObject.get("start").getAsString();
                start = ByteKey.copyFrom(startKey.getBytes(StandardCharsets.UTF_8));
            } else {
                start = ByteKey.EMPTY;
            }

            final ByteKey end;
            if(jsonObject.has("end")) {
                final String endKey = jsonObject.get("end").getAsString();
                end = ByteKey.copyFrom(endKey.getBytes(StandardCharsets.UTF_8));
            } else {
                end = ByteKey.EMPTY;
            }

            return Arrays.asList(ByteKeyRange.of(start, end));
        } else if(jsonElement.isJsonArray()) {
            final List<ByteKeyRange> ranges = new ArrayList<>();
            for(final JsonElement element : jsonElement.getAsJsonArray()) {
                final List<ByteKeyRange> range = createKeyRanges(element);
                ranges.addAll(range);
            }

            return ranges;
        } else {
            throw new IllegalArgumentException("Illegal keyRange format: " + jsonElement);
        }
    }


    public static RowFilter createRowFilter(final JsonElement jsonElement) {
        if(jsonElement == null || jsonElement.isJsonNull()) {
            return RowFilter.newBuilder().setPassAllFilter(true).build();
        } else if(jsonElement.isJsonPrimitive()) {
            throw new IllegalArgumentException("Illegal rowFilter: " + jsonElement);
        }

        if(jsonElement.isJsonArray()) {
            final List<RowFilter> children = new ArrayList<>();
            for(JsonElement child : jsonElement.getAsJsonArray()) {
                children.add(createRowFilter(child));
            }
            return RowFilter.newBuilder()
                    .setChain(RowFilter.Chain.newBuilder().addAllFilters(children).build())
                    .build();
        } else if(jsonElement.isJsonObject()) {
            final JsonObject jsonObject = jsonElement.getAsJsonObject();
            if(!jsonObject.has("type")) {
                throw new IllegalArgumentException("rowFilter requires type parameter");
            }
            final FilterType filterType = FilterType.valueOf(jsonObject.get("type").getAsString());
            return switch (filterType) {
                case sample -> {
                    if(!jsonObject.has("rate")) {
                        throw new IllegalArgumentException("filterType: " + filterType + " requires rate parameter");
                    }
                    final double sample = jsonObject.get("sample").getAsDouble();
                    yield RowFilter.newBuilder().setRowSampleFilter(sample).build();
                }
                case row_key_regex, family_name_regex, column_qualifier_regex, value_regex -> {
                    if(!jsonObject.has("regex")) {
                        throw new IllegalArgumentException("filterType: " + filterType + " requires regex parameter");
                    }
                    final String regex = jsonObject.get("regex").getAsString();
                    final ByteString byteString = ByteString.copyFromUtf8(regex);
                    yield switch (filterType) {
                        case row_key_regex -> RowFilter.newBuilder().setRowKeyRegexFilter(byteString).build();
                        case family_name_regex -> RowFilter.newBuilder().setFamilyNameRegexFilter(regex).build();
                        case column_qualifier_regex -> RowFilter.newBuilder().setColumnQualifierRegexFilter(byteString).build();
                        case value_regex -> RowFilter.newBuilder().setValueRegexFilter(byteString).build();
                        default -> throw new IllegalArgumentException("filterType: " + filterType + " is not supported");
                    };
                }
                case limit_cells_per_row, limit_cells_per_column -> {
                    if(!jsonObject.has("limit")) {
                        throw new IllegalArgumentException("filterType: " + filterType + " requires limit parameter");
                    }
                    final int limit = jsonObject.get("limit").getAsInt();
                    yield switch (filterType) {
                        case limit_cells_per_row -> RowFilter.newBuilder().setCellsPerRowLimitFilter(limit).build();
                        case limit_cells_per_column -> RowFilter.newBuilder().setCellsPerColumnLimitFilter(limit).build();
                        default -> throw new IllegalArgumentException("filterType: " + filterType + " is not supported");
                    };
                }
                case offset_cells_per_row -> {
                    if(!jsonObject.has("offset")) {
                        throw new IllegalArgumentException("filterType: " + filterType + " requires offset parameter");
                    }
                    final int offset = jsonObject.get("offset").getAsInt();
                    yield RowFilter.newBuilder().setCellsPerRowOffsetFilter(offset).build();
                }
                case column_range, value_range -> {
                    final boolean startIsOpen;
                    final boolean endIsOpen;
                    final JsonElement start;
                    final JsonElement end;
                    if(jsonObject.has("startOpen")) {
                        start = jsonObject.get("startOpen");
                        startIsOpen = true;
                    } else if(jsonObject.has("startClosed")) {
                        start = jsonObject.get("startClosed");
                        startIsOpen = false;
                    } else {
                        start = null;
                        startIsOpen = false;
                    }
                    if(jsonObject.has("endOpen")) {
                        end = jsonObject.get("endOpen");
                        endIsOpen = true;
                    } else if(jsonObject.has("endClosed")) {
                        end = jsonObject.get("endOpen");
                        endIsOpen = false;
                    } else {
                        end = null;
                        endIsOpen = false;
                    }
                    if(start == null && end == null) {
                        throw new IllegalArgumentException("filterType: " + filterType + " requires startOpen or endOpen");
                    }

                    yield switch (filterType) {
                        case column_range -> {
                            ColumnRange.Builder builder = ColumnRange.newBuilder();
                            if(jsonObject.has("family")) {
                                final String familyName = jsonObject.get("family").getAsString();
                                builder = builder.setFamilyName(familyName);
                            }
                            if(start != null) {
                                final ByteString startByteString = ByteString.copyFrom(start.getAsString(), StandardCharsets.UTF_8);
                                if(startIsOpen) {
                                    builder = builder.setStartQualifierOpen(startByteString);
                                } else {
                                    builder = builder.setStartQualifierClosed(startByteString);
                                }
                            }
                            if(end != null) {
                                final ByteString endByteString = ByteString.copyFrom(end.getAsString(), StandardCharsets.UTF_8);
                                if(endIsOpen) {
                                    builder = builder.setEndQualifierOpen(endByteString);
                                } else {
                                    builder = builder.setEndQualifierClosed(endByteString);
                                }
                            }
                            yield RowFilter.newBuilder().setColumnRangeFilter(builder.build()).build();
                        }
                        case value_range -> {
                            ValueRange.Builder builder = ValueRange.newBuilder();
                            if(start != null) {
                                final ByteString startByteString = ByteString.copyFrom(start.getAsString(), StandardCharsets.UTF_8);
                                if(startIsOpen) {
                                    builder = builder.setStartValueOpen(startByteString);
                                } else {
                                    builder = builder.setStartValueClosed(startByteString);
                                }
                            }
                            if(end != null) {
                                final ByteString endByteString = ByteString.copyFrom(end.getAsString(), StandardCharsets.UTF_8);
                                if(endIsOpen) {
                                    builder = builder.setEndValueOpen(endByteString);
                                } else {
                                    builder = builder.setEndValueClosed(endByteString);
                                }
                            }
                            yield RowFilter.newBuilder().setValueRangeFilter(builder.build()).build();
                        }
                        default -> throw new IllegalArgumentException("filterType: " + filterType + " is not supported");
                    };
                }

                case timestamp_range -> {
                    final String startTimestamp;
                    final String endTimestamp;
                    if(jsonObject.has("start")) {
                        startTimestamp = jsonObject.get("start").getAsString();
                    } else {
                        startTimestamp = null;
                    }
                    if(jsonObject.has("end")) {
                        endTimestamp = jsonObject.get("end").getAsString();
                    } else {
                        endTimestamp = null;
                    }

                    TimestampRange.Builder builder = TimestampRange.newBuilder();
                    if(startTimestamp != null) {
                        final long startMicros = DateTimeUtil.toEpochMicroSecond(startTimestamp);
                        builder = builder.setStartTimestampMicros(startMicros);
                    }
                    if(endTimestamp != null) {
                        final long endMicros = DateTimeUtil.toEpochMicroSecond(endTimestamp);
                        builder = builder.setEndTimestampMicros(endMicros);
                    }
                    yield RowFilter.newBuilder().setTimestampRangeFilter(builder.build()).build();
                }
                case block, pass, sink, strip -> {
                    final boolean flag;
                    if(jsonObject.has("flag")) {
                        flag = jsonObject.get("flag").getAsBoolean();
                    } else {
                        flag = true;
                    }
                    yield switch (filterType) {
                        case block -> RowFilter.newBuilder().setBlockAllFilter(flag).build();
                        case pass -> RowFilter.newBuilder().setPassAllFilter(flag).build();
                        case sink -> RowFilter.newBuilder().setSink(flag).build();
                        case strip -> RowFilter.newBuilder().setStripValueTransformer(flag).build();
                        default -> throw new IllegalArgumentException("filterType: " + filterType + " is not supported");
                    };
                }
                case label -> {
                    if(!jsonObject.has("label")) {
                        throw new IllegalArgumentException("filterType: " + filterType + " requires label parameter");
                    }
                    final String label = jsonObject.get("label").getAsString();
                    yield RowFilter.newBuilder().setApplyLabelTransformer(label).build();
                }
                case chain, interleave -> {
                    if(jsonObject.has("children")) {
                        throw new IllegalArgumentException("filterType: " + filterType + " requires children parameter");
                    }
                    final JsonElement childrenElement = jsonObject.get("children");
                    if(!childrenElement.isJsonArray()) {
                        throw new IllegalArgumentException("filterType: " + filterType + " children parameter must be array of rowFilter");
                    }
                    final JsonArray children = childrenElement.getAsJsonArray();
                    final List<RowFilter> filters = new ArrayList<>();
                    for(JsonElement child : children) {
                        filters.add(createRowFilter(child));
                    }
                    yield switch (filterType) {
                        case chain -> RowFilter.newBuilder().setChain(RowFilter.Chain.newBuilder().addAllFilters(filters).build()).build();
                        case interleave -> RowFilter.newBuilder().setInterleave(RowFilter.Interleave.newBuilder().addAllFilters(filters).build()).build();
                        default -> throw new IllegalArgumentException("filterType: " + filterType + " is not supported");
                    };
                }
                case condition -> throw new IllegalArgumentException("Not supported condition");
                default -> throw new IllegalArgumentException("filterType: " + filterType + " is not supported");
            };
        } else {
            throw new IllegalArgumentException();
        }
    }

    public static ByteKeyRange createSimpleKeyRange(final String str) {
        if(str == null || str.isEmpty()) {
            return ByteKeyRange.ALL_KEYS;
        }
        final String[] keys = str.split(",");
        final ByteKey start = ByteKey.copyFrom(keys[0].getBytes(StandardCharsets.UTF_8));
        final ByteKey end;
        if(keys.length > 1) {
            end = ByteKey.copyFrom(keys[1].getBytes(StandardCharsets.UTF_8));
        } else {
            end = ByteKey.EMPTY;
        }

        return ByteKeyRange.of(start, end);
    }

    public static List<ByteKeyRange> createSimpleKeyRanges(final String str) {
        if(str == null || str.isEmpty()) {
            return Arrays.asList(ByteKeyRange.ALL_KEYS);
        }
        final String[] keys = str.split(",");
        final ByteKey start = ByteKey.copyFrom(keys[0].getBytes(StandardCharsets.UTF_8));
        final ByteKey end;
        if(keys.length > 1) {
            end = ByteKey.copyFrom(keys[1].getBytes(StandardCharsets.UTF_8));
        } else {
            end = ByteKey.EMPTY;
        }

        return Arrays.asList(ByteKeyRange.of(start, end));
    }

    public static RowFilter createSimpleRowFilter(final String str) {
        if(str == null) {
            return RowFilter.newBuilder().setPassAllFilter(true).build();
        }
        final String[] strs = str.split(",");
        final List<RowFilter> filters = new ArrayList<>();
        for(String s : strs) {
            final String strValue = s
                    .trim()
                    .replaceAll("\"","")
                    .replaceAll("'","");
            filters.add(RowFilter.newBuilder().setRowKeyRegexFilter(ByteString.copyFromUtf8(strValue)).build());
        }

        if(filters.isEmpty()) {
            return RowFilter.newBuilder().setPassAllFilter(true).build();
        } else if(filters.size() == 1) {
            return filters.getFirst();
        } else {
            return RowFilter.newBuilder().setChain(RowFilter.Chain.newBuilder().addAllFilters(filters).build()).build();
        }
    }

}
