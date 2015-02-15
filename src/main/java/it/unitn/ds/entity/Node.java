package it.unitn.ds.entity;

import com.google.common.base.MoreObjects;

import java.io.Serializable;
import java.util.*;

/**
 * Nodes are put in the ring in acceding order (with the most greatest id followed by the most lowest id, forming a ring)
 * Nodes store items, such that (NodeId >= itemKey) and replicas of N predecessor's node items
 *
 * @see it.unitn.ds.entity.Item
 * @see it.unitn.ds.Replication
 */
public final class Node implements Serializable {

    /**
     * Positive integer to determine position in the ring
     */
    private final int id;

    /**
     * IP address of the server node
     */
    private final String host;

    /**
     * Own items, for which the node is responsible for
     * <p/>
     * Map<ItemKey, Item>
     */
    private final Map<Integer, Item> items = new TreeMap<>();

    /**
     * Replicated items from predecessor nodes
     * <p/>
     * Map<ItemKey, Item>
     */
    private final Map<Integer, Item> replicas = new TreeMap<>();

    /**
     * All known nodes in the ring, including itself
     * <p/>
     * Map<NodeId, Host>
     */
    // TODO should this be a double linked list instead ?
    private final Map<Integer, String> nodes = new TreeMap<>();

    public Node() {
        this.id = 0;
        this.host = "";
    }

    public Node(int id, String host) {
        this.id = id;
        this.host = host;
        nodes.put(id, host);
    }

    public Node(Node node) {
        this(node.id, node.host);
    }

    public void putNodes(Map<Integer, String> nodes) {
        this.nodes.putAll(nodes);
    }

    public void putNode(int id, String host) {
        nodes.put(id, host);
    }

    public void removeNode(int id) {
        nodes.remove(id);
    }

    public void putItems(List<Item> items) {
        for (Item item : items) {
            this.items.put(item.getKey(), item);
        }
    }

    public void removeItems(List<Item> items) {
        for (Item item : items) {
            this.items.remove(item.getKey());
        }
    }

    public void putReplicas(List<Item> replicas) {
        for (Item replica : replicas) {
            this.replicas.put(replica.getKey(), replica);
        }
    }

    public void removeReplicas(List<Item> replicas) {
        for (Item replica : replicas) {
            this.replicas.remove(replica.getKey());
        }
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

    public Map<Integer, Item> getReplicas() {
        return Collections.unmodifiableMap(replicas);
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
                .add("replicas", Arrays.toString(replicas.keySet().toArray()))
                .add("nodes", Arrays.toString(nodes.entrySet().toArray()))
                .toString();
    }
}
