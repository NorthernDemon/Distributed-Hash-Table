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

    private final int nodeId;

    public Item() {
        this.key = 0;
        this.value = "";
        this.version = 0;
        this.nodeId = 0;
    }

    public Item(int key, String value, int version, int nodeId) {
        this.key = key;
        this.value = value;
        this.version = version;
        this.nodeId = nodeId;
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

    public int getNodeId() {
        return nodeId;
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
                .add("nodeId", nodeId)
                .toString();
    }
}
