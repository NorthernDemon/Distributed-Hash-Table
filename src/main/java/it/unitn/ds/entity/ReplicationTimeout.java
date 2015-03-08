package it.unitn.ds.entity;

import com.google.common.base.MoreObjects;
import org.jetbrains.annotations.NotNull;

import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * Replication timeout is used for fast client get/update operations on replicas
 *
 * @see it.unitn.ds.Replication
 * @see it.unitn.ds.util.MultithreadingUtil
 */
public final class ReplicationTimeout {

    private int value;

    @NotNull
    private TimeUnit unit;

    public ReplicationTimeout(int value, @NotNull String unit) {
        this.value = value;
        this.unit = TimeUnit.valueOf(unit);
    }

    public int getValue() {
        return value;
    }

    @NotNull
    public TimeUnit getUnit() {
        return unit;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        if (o instanceof ReplicationTimeout) {
            ReplicationTimeout object = (ReplicationTimeout) o;

            return Objects.equals(value, object.value) &&
                    Objects.equals(unit, object.unit);
        }

        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hash(value, unit);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("value", value)
                .add("unit", unit.name())
                .toString();
    }
}
