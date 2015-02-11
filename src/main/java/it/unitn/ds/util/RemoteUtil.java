package it.unitn.ds.util;

import it.unitn.ds.Replication;
import it.unitn.ds.entity.Item;
import it.unitn.ds.entity.Node;
import it.unitn.ds.rmi.NodeServer;
import it.unitn.ds.rmi.NullNodeRemote;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.Nullable;

import java.rmi.Naming;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Convenient class to deal with RMI for nodes
 */
public abstract class RemoteUtil {

    private static final Logger logger = LogManager.getLogger();

    /**
     * Get reference to remote node given node id
     *
     * @param node  current node
     * @param clazz type of the interface
     * @return reference to remote object
     */
    private static <T> T getRemoteNode(Node node, Class<T> clazz) {
        return getRemoteNode(node.getHost(), node.getId(), clazz);
    }

    /**
     * Get reference to remote node given node id
     *
     * @param host   network host
     * @param nodeId of the wanted node
     * @param clazz  type of the interface
     * @return reference to remote object
     */
    public static <T> T getRemoteNode(String host, int nodeId, Class<T> clazz) {
        try {
            return clazz.cast(Naming.lookup(getRMI(host, nodeId)));
        } catch (Exception e) {
            logger.error("Failed to get Remote Interface for nodeId=" + nodeId, e);
            try {
                return clazz.cast(new NullNodeRemote());
            } catch (RemoteException re) {
                logger.error("Failed to get Null Node Pattern for nodeId=" + nodeId, re);
                return null;
            }
        }
    }

    /**
     * Get node id for given item key in the set of nodes
     *
     * @param key   of the item
     * @param nodes set of possible nodes
     * @return node id
     */
    public static int getNodeIdForItemKey(int key, Map<Integer, String> nodes) {
        for (int nodeId : nodes.keySet()) {
            if (nodeId >= key) {
                return nodeId;
            }
        }
        return nodes.keySet().iterator().next();
    }

    /**
     * Returns successor node clockwise in the ring of the given current node
     *
     * @param node current node
     * @return successor node of the given node id
     */
    @Nullable
    public static Node getSuccessorNode(Node node) throws RemoteException {
        return getSuccessorNode(node.getId(), node.getNodes());
    }

    /**
     * Returns successor node clockwise in the ring of the given node id in the set of nodes
     *
     * @param nodeId predecessor node id
     * @param nodes  set of possible successors
     * @return successor node of the given node id
     */
    @Nullable
    public static Node getSuccessorNode(int nodeId, Map<Integer, String> nodes) throws RemoteException {
        int successorNodeId = getSuccessorNodeId(nodeId, nodes);
        if (successorNodeId == nodeId) {
            logger.info("NodeId=" + nodeId + " is a successor to itself!");
            return null;
        }
        logger.debug("NodeId=" + nodeId + " found successorNodeId=" + successorNodeId);
        return getRemoteNode(nodes.get(successorNodeId), successorNodeId, NodeServer.class).getNode();
    }

    private static int getSuccessorNodeId(int targetNodeId, Map<Integer, String> nodes) {
        for (int nodeId : nodes.keySet()) {
            if (nodeId > targetNodeId) {
                return nodeId;
            }
        }
        return nodes.keySet().iterator().next();
    }

    private static int getPredecessorNodeId(int targetNodeId, Map<Integer, String> nodes) {
        for (int nodeId : nodes.keySet()) {
            if (nodeId <= targetNodeId) {
                return nodeId;
            }
        }
        return nodes.keySet().iterator().next();
    }

    /**
     * Copies items from current node to successor node
     *
     * @param fromNode from which to transfer
     * @param toNode   to which to transfer
     */
    public static void copyItems(Node fromNode, Node toNode) throws RemoteException {
        copyItems(toNode, new ArrayList<>(fromNode.getItems().values()));
    }

    private static void copyItems(Node node, List<Item> items) throws RemoteException {
        if (items.isEmpty()) {
            logger.debug("Nothing to copy to node=" + node);
            return;
        }
        getRemoteNode(node, NodeServer.class).updateItems(items);
        logger.debug("Copied items=" + Arrays.toString(items.toArray()) + " node=" + node);
    }

    /**
     * Transfers items from successor node to current node, removes items of fromNode
     *
     * @param fromNode from which to transfer
     * @param toNode   to which to transfer
     */
    public static void transferItems(Node fromNode, Node toNode) throws RemoteException {
        if (fromNode.getItems().isEmpty()) {
            logger.debug("Nothing to transfer from fromNode=" + fromNode + " toNode=" + toNode);
            return;
        }
        List<Item> items = getTransferItems(fromNode, toNode);
        getRemoteNode(toNode, NodeServer.class).updateItems(items);
        getRemoteNode(fromNode, NodeServer.class).removeItems(items);
        logger.debug("Transferred items fromNode=" + fromNode + " node=" + toNode + " items=" + Arrays.toString(items.toArray()));
    }

    /**
     * Returns a list of items, that should be transferred from successor node to current node
     * <p/>
     * If new node appears the last and successor it the first (zero-crossing)
     * it transfer only items between current node and its predecessor
     *
     * @param fromNode from which to transfer
     * @param toNode   to which to transfer
     * @return list of items for current node
     */
    private static List<Item> getTransferItems(Node fromNode, Node toNode) {
        int predecessorNodeId = getPredecessorNodeId(toNode.getId(), toNode.getNodes());
        List<Item> items = new ArrayList<>();
        for (Item item : fromNode.getItems().values()) {
            // check if item (e.g. 5) falls in range of highest-identified node (e.g.20) or lowest (e.g. 5)
            if (toNode.getId() >= item.getKey() && item.getKey() > predecessorNodeId) {
                items.add(item);
            }
        }
        return items;
    }

    public static List<Item> getReplicas(Map<Integer, String> nodes, int key) throws RemoteException {
        Node node = getSuccessorNode(getNodeIdForItemKey(key, nodes) - 1, nodes);
        logger.debug("Getting replication N=" + Replication.N + " from node=" + node);
        List<Item> items = new ArrayList<>(Replication.N);
        for (int i = 0; i < Replication.N; i++) {
            if (node != null && i != Replication.R) {
                Item item = node.getItems().get(key);
                if (item != null) {
                    items.add(item);
                    logger.debug("Got replicas i=" + i + " of item=" + item + " from node=" + node);
                    node = getSuccessorNode(node);
                }
            }
        }
        return items;
    }

    public static void updateReplicas(Map<Integer, String> nodes, final Item item) throws RemoteException {
        Node node = getSuccessorNode(getNodeIdForItemKey(item.getKey(), nodes) - 1, nodes);
        logger.debug("Updating replicas N=" + Replication.N + " from node=" + node);
        for (int i = 0; i < Replication.N; i++) {
            if (node != null) {
                copyItems(node, new ArrayList<Item>() {{
                    add(item);
                }});
                logger.debug("Replicated i=" + i + " to node=" + node);
                node = getSuccessorNode(node);
            }
        }
    }

    /**
     * Returns RMI string of the node for given host
     *
     * @param host   network host
     * @param nodeId id of the current node
     * @return default lookup string
     */
    public static String getRMI(String host, int nodeId) {
        return "rmi://" + host + "/NodeRemote" + nodeId;
    }
}
