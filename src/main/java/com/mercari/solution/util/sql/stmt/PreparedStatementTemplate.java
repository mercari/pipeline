package com.mercari.solution.util.sql.stmt;

import java.io.Serializable;
import java.math.BigDecimal;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

public class PreparedStatementTemplate implements Serializable {
    private final String statement;
    private final PlaceholderMappings placeholderMappings;

    private PreparedStatementTemplate(String statement, PlaceholderMappings placeholderMappings) {
        this.statement = statement;
        this.placeholderMappings = placeholderMappings;
    }

    public String getStatementString() {
        return this.statement;
    }

    public PlaceholderMappings getPlaceholderMappings() {
        return this.placeholderMappings;
    }

    @Override
    public String toString() {
        return this.statement;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PreparedStatementTemplate that = (PreparedStatementTemplate) o;
        return Objects.equals(statement, that.statement) && Objects.equals(placeholderMappings, that.placeholderMappings);
    }

    @Override
    public int hashCode() {
        return Objects.hash(statement, placeholderMappings);
    }

    public PlaceholderSetterProxy createPlaceholderSetterProxy(PreparedStatement preparedStatement) {
        return new PlaceholderSetterProxy(preparedStatement, this.placeholderMappings);
    }

    public static class PlaceholderSetterProxy {
        private final PreparedStatement preparedStatement;
        private final PlaceholderMappings placeholderMappings;

        PlaceholderSetterProxy(PreparedStatement preparedStatement, PlaceholderMappings placeholderMappings) {
            this.preparedStatement = preparedStatement;
            this.placeholderMappings = placeholderMappings;
        }

        public void setString(int index, String value) throws java.sql.SQLException {
            List<Integer> mappedIndices = placeholderMappings.getMappings().get(index);
            for (Integer placeholderIndex : mappedIndices) {
                preparedStatement.setString(placeholderIndex, value);
            }
        }

        public void setShort(int index, short value) throws java.sql.SQLException {
            List<Integer> mappedIndices = placeholderMappings.getMappings().get(index);
            for (Integer placeholderIndex : mappedIndices) {
                preparedStatement.setShort(placeholderIndex, value);
            }
        }

        public void setInt(int index, int value) throws java.sql.SQLException  {
            List<Integer> mappedIndices = placeholderMappings.getMappings().get(index);
            for (Integer placeholderIndex : mappedIndices) {
                preparedStatement.setInt(placeholderIndex, value);
            }
        }

        public void setLong(int index, long value) throws java.sql.SQLException  {
            List<Integer> mappedIndices = placeholderMappings.getMappings().get(index);
            for (Integer placeholderIndex : mappedIndices) {
                preparedStatement.setLong(placeholderIndex, value);
            }
        }

        public void setFloat(int index, float value) throws java.sql.SQLException  {
            List<Integer> mappedIndices = placeholderMappings.getMappings().get(index);
            for (Integer placeholderIndex : mappedIndices) {
                preparedStatement.setFloat(placeholderIndex, value);
            }
        }

        public void setDouble(int index, double value) throws java.sql.SQLException  {
            List<Integer> mappedIndices = placeholderMappings.getMappings().get(index);
            for (Integer placeholderIndex : mappedIndices) {
                preparedStatement.setDouble(placeholderIndex, value);
            }
        }

        public void setBigDecimal(int index, BigDecimal value) throws java.sql.SQLException  {
            List<Integer> mappedIndices = placeholderMappings.getMappings().get(index);
            for (Integer placeholderIndex : mappedIndices) {
                preparedStatement.setBigDecimal(placeholderIndex, value);
            }
        }

        public void setBoolean(int index, boolean value) throws java.sql.SQLException  {
            List<Integer> mappedIndices = placeholderMappings.getMappings().get(index);
            for (Integer placeholderIndex : mappedIndices) {
                preparedStatement.setBoolean(placeholderIndex, value);
            }
        }

        public void setTime(int index, Time value) throws java.sql.SQLException  {
            List<Integer> mappedIndices = placeholderMappings.getMappings().get(index);
            for (Integer placeholderIndex : mappedIndices) {
                preparedStatement.setTime(placeholderIndex, value);
            }
        }

        public void setTimestamp(int index, Timestamp value) throws java.sql.SQLException  {
            List<Integer> mappedIndices = placeholderMappings.getMappings().get(index);
            for (Integer placeholderIndex : mappedIndices) {
                preparedStatement.setTimestamp(placeholderIndex, value);
            }
        }

        public void setDate(int index, Date value) throws java.sql.SQLException {
            List<Integer> mappedIndices = placeholderMappings.getMappings().get(index);
            for (Integer placeholderIndex : mappedIndices) {
                preparedStatement.setDate(placeholderIndex, value);
            }
        }

        public void setNull(int index, int sqlType) throws java.sql.SQLException {
            List<Integer> mappedIndices = placeholderMappings.getMappings().get(index);
            for (Integer placeholderIndex : mappedIndices) {
                preparedStatement.setNull(placeholderIndex, sqlType);
            }
        }

        public void setBytes(int index, byte[] value) throws java.sql.SQLException {
            List<Integer> mappedIndices = placeholderMappings.getMappings().get(index);
            for (Integer placeholderIndex : mappedIndices) {
                preparedStatement.setBytes(placeholderIndex, value);
            }
        }

        public void setUUID(int index, String value) throws java.sql.SQLException {
            List<Integer> mappedIndices = placeholderMappings.getMappings().get(index);
            for (Integer placeholderIndex : mappedIndices) {
                final UUID uuid = UUID.fromString(value);
                preparedStatement.setObject(placeholderIndex, uuid, Types.OTHER);
            }
        }

        public void setObject(int index, Object value) throws java.sql.SQLException {
            List<Integer> mappedIndices = placeholderMappings.getMappings().get(index);
            for (Integer placeholderIndex : mappedIndices) {
                preparedStatement.setObject(placeholderIndex, value, Types.OTHER);
            }
        }
    }

    private static class Placeholder {
        private final int index;

        public Placeholder(int index) {
            this.index = index;
        }

        public int getIndex() {
            return index;
        }
    }

    public static class PlaceholderMappings implements Serializable {
        private final List<List<Integer>> mappings;

        public PlaceholderMappings(List<List<Integer>> mappings) {
            this.mappings = mappings;
        }

        public List<List<Integer>> getMappings() {
            return this.mappings;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            PlaceholderMappings that = (PlaceholderMappings) o;
            return Objects.equals(mappings, that.mappings);
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(mappings);
        }
    }

    public static class Builder {
        private final List<Object> buffer = new ArrayList<>();

        public Builder appendString(String value) {
            this.buffer.add(value);
            return this;
        }

        public Builder appendSingleQuoted(String value) {
            this.buffer.add("'");
            this.buffer.add(value);
            this.buffer.add("'");
            return this;
        }

        public Builder appendDoubleQuoted(String value) {
            this.buffer.add("\"");
            this.buffer.add(value);
            this.buffer.add("\"");
            return this;
        }

        public Builder appendBackQuoted(String value) {
            this.buffer.add("`");
            this.buffer.add(value);
            this.buffer.add("`");
            return this;
        }

        public Builder appendPlaceholder(int index) {
            this.buffer.add(new Placeholder(index));
            return this;
        }

        public Builder removeLast() {
            int n = this.buffer.size();
            if (n > 0) {
                this.buffer.remove(n - 1);
            }
            return this;
        }

        public Builder reset() {
            this.buffer.clear();
            return this;
        }

        public PreparedStatementTemplate build() {
            return new PreparedStatementTemplate(this.buildStatementText(), this.buildPlaceholderMappings());
        }

        private String buildStatementText() {
            final StringBuilder sb = new StringBuilder();

            this.buffer.forEach((fragment) -> {
                if (fragment instanceof String) {
                    sb.append((String) fragment);
                } else if (fragment instanceof Placeholder) {
                    sb.append("?");
                } else {
                    throw new IllegalStateException("Unknown statement fragment type: " + fragment.getClass().getName());
                }
            });

            return sb.toString();
        }

        private PlaceholderMappings buildPlaceholderMappings() {
            int maxIndex = 0;

            for (Object o : buffer) {
                if (o instanceof Placeholder) {
                    maxIndex = Math.max(maxIndex, ((Placeholder) o).getIndex());
                }
            }

            if (maxIndex == 0) {
                return new PlaceholderMappings(new ArrayList<>());
            }

            final List<List<Integer>> mappings = new ArrayList<>();

            for (int i = 0; i < (maxIndex + 1); i++) {
                mappings.add(new ArrayList<>());
            }

            int rawIndex = 0;
            for (Object o : this.buffer) {
                if (o instanceof Placeholder) {
                    rawIndex++;
                    mappings.get(((Placeholder) o).getIndex()).add(rawIndex);
                }
            }

            return new PlaceholderMappings(mappings);
        }
    }
}

