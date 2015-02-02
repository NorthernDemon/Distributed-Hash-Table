package it.unitn.ds.server;

import com.google.common.base.MoreObjects;

import java.io.Serializable;
import java.util.*;

public final class Node implements Serializable {

    private final int id;

    private final String host;

    /**
     * Map<ItemKey, Item>
     */
    private final Map<Integer, Item> items = new TreeMap<>();

    /**
     * Map<NodeId, Host>, including itself
     */
    private final Map<Integer, String> nodes = new TreeMap<>();

    public Node(int id, String host) {
        this.id = id;
        this.host = host;
    }

    public void addNodes(Map<Integer, String> existingNodes) {
        addNodes();
        nodes.putAll(existingNodes);
    }

    public void addNodes() {
        nodes.put(id, host);
    }

    public void putNode(int nodeId, String host) {
        nodes.put(nodeId, host);
    }

    public void removeNode(int nodeId) {
        nodes.remove(nodeId);
    }

    public void putItem(Item item) {
        items.put(item.getKey(), item);
    }

    public void removeItem(int itemKey) {
        items.remove(itemKey);
    }

    public int getId() {
        return id;
    }

    public String getHost() {
        return host;
    }

    public Map<Integer, Item> getItems() {
        return Collections.unmodifiableMap(items);
    }

    public Map<Integer, String> getNodes() {
        return Collections.unmodifiableMap(nodes);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        if (o instanceof Node) {
            Node object = (Node) o;

            return Objects.equals(id, object.id);
        }

        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("id", id)
                .add("host", host)
                .add("items", Arrays.toString(items.keySet().toArray()))
                .add("nodes", Arrays.toString(nodes.entrySet().toArray()))
                .toString();
    }
}
