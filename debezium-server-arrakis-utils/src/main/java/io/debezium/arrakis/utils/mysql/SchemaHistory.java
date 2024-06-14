package io.debezium.arrakis.utils.mysql;

import java.util.Objects;

public class SchemaHistory<T> {
    private final T schema;
    private final boolean isCompressed;

    public SchemaHistory(T schema, boolean isCompressed) {
        this.schema = schema;
        this.isCompressed = isCompressed;
    }

    public T getSchema() {
        return schema;
    }

    public boolean isCompressed() {
        return isCompressed;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        SchemaHistory<?> that = (SchemaHistory<?>) o;
        return isCompressed == that.isCompressed && schema.equals(that.schema);
    }

    @Override
    public int hashCode() {
        return Objects.hash(schema, isCompressed);
    }

    @Override
    public String toString() {
        return "SchemaHistory{" +
                "schema=" + schema +
                ", isCompressed=" + isCompressed +
                '}';
    }
}
