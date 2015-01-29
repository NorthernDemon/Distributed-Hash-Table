package it.unitn.ds.server;

import com.google.common.base.MoreObjects;

import java.io.Serializable;
import java.util.Objects;

public class Item implements Serializable {

    private int key;

    private String value;

    private int version;

    public Item(String value, int version) {
        this.value = value;
        this.version = version;
    }

    public Item(int key, String value, int version) {
        this(value, version);
        this.key = key;
    }

    public int getKey() {
        return key;
    }

    public void setKey(int key) {
        this.key = key;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public int getVersion() {
        return version;
    }

    public void setVersion(int version) {
        this.version = version;
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
