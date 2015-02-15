package it.unitn.ds.entity;

import com.google.common.base.MoreObjects;

import java.io.Serializable;
import java.util.Objects;

/**
 * Items are put in the ring under the responsible node (NodeId >= itemKey) and replicated to N successors
 *
 * @see it.unitn.ds.entity.Node
 * @see it.unitn.ds.Replication
 */
public final class Item implements Serializable {

    /**
     * Positive integer to determine responsible node in the ring
     */
    private final int key;

    /**
     * Item value without commas
     */
    private final String value;

    /**
     * Used in replication to determine the latest item, starts from 1 and up
     */
    private final int version;

    public Item(int key, String value, int version) {
        this.key = key;
        this.value = value;
        this.version = version;
    }

    public int getKey() {
        return key;
    }

    public String getValue() {
        return value;
    }

    public int getVersion() {
        return version;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        if (o instanceof Item) {
            Item object = (Item) o;

            return Objects.equals(key, object.key) &&
                    Objects.equals(version, object.version);
        }

        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hash(key, version);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("key", key)
                .add("value", value)
                .add("version", version)
                .toString();
    }
}
