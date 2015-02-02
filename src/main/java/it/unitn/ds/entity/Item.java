package it.unitn.ds.entity;

import com.google.common.base.MoreObjects;

import java.io.Serializable;
import java.util.Objects;

/**
 * Item stored in the ring based on its key, falls into area of nodes
 * Version is used in replication, starts from 1 and up
 * Clients can get/update item given it's key and at least one node
 * Coordinator does not have to store the item himself
 * Responsible node has id >= item key
 */
public final class Item implements Serializable {

    private final int key;

    private final String value;

    private final int version;

    public Item(int key, String value, int version) {
        this.value = value;
        this.version = version;
        this.key = key;
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

            return Objects.equals(key, object.key);
        }

        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hash(key);
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
