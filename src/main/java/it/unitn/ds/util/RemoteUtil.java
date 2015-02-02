package it.unitn.ds.util;

import it.unitn.ds.Replication;
import it.unitn.ds.server.Item;
import it.unitn.ds.server.Node;
import it.unitn.ds.server.NodeRemote;
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
     * @param host   network host
     * @param nodeId of the wanted node
     * @return reference to remote object
     */
    @Nullable
    public static NodeRemote getRemoteNode(String host, int nodeId) {
        try {
            return (NodeRemote) Naming.lookup(getRMI(host, nodeId));
        } catch (Exception e) {
            logger.error("Failed to get remote node by nodeId=" + nodeId, e);
        }
        return null;
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
     * Returns successor node clockwise in circle of the given node id in the set of nodes
     *
     * @param nodeId predecessor node id
     * @param nodes  set of possible successors
     * @return successor node of the given node id
     */
    @Nullable
    public static Node getSuccessorNode(int nodeId, Map<Integer, String> nodes) throws RemoteException {
        int successorNodeId = getSuccessorNodeId(nodeId, nodes);
        if (successorNodeId == nodeId) {
            logger.debug("NodeId=" + nodeId + " did not find successorNode, except itself");
            return null;
        }
        logger.debug("NodeId=" + nodeId + " found successorNodeId=" + successorNodeId);
        NodeRemote remoteNode = RemoteUtil.getRemoteNode(nodes.get(successorNodeId), successorNodeId);
        if (remoteNode == null) {
            logger.warn("Cannot get remote nodeId=" + successorNodeId);
            return null;
        } else {
            return remoteNode.getNode();
        }
    }

    private static int getSuccessorNodeId(int targetNodeId, Map<Integer, String> nodes) {
        for (int nodeId : nodes.keySet()) {
            if (nodeId > targetNodeId) {
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
        copyItems(fromNode, new ArrayList<>(fromNode.getItems().values()), toNode);
    }

    public static void copyItems(Node fromNode, List<Item> items, Node toNode) throws RemoteException {
        if (fromNode.getItems().isEmpty()) {
            logger.debug("Nothing to copy from fromNode=" + fromNode + " toNode=" + toNode);
            return;
        }
        NodeRemote remoteNode = RemoteUtil.getRemoteNode(toNode.getHost(), toNode.getId());
        if (remoteNode == null) {
            logger.warn("Cannot get remote toNodeId=" + toNode.getId());
            return;
        }
        remoteNode.updateItems(items);
        logger.debug("Copied items fromNode=" + fromNode + " toNode=" + toNode);
    }

    /**
     * Transfers items from successor node to current node, removes items of fromNode
     *
     * @param fromNode from which to transfer
     * @param toNode   from which to transfer
     */
    public static void transferItems(Node fromNode, Node toNode) throws RemoteException {
        if (fromNode.getItems().isEmpty()) {
            logger.debug("Nothing to transfer from fromNode=" + fromNode + " toNode=" + toNode);
            return;
        }
        List<Item> items = getTransferItems(fromNode, toNode);
        NodeRemote remoteNode = RemoteUtil.getRemoteNode(toNode.getHost(), toNode.getId());
        if (remoteNode == null) {
            logger.warn("Cannot get remote toNodeId=" + toNode.getId());
            return;
        }
        remoteNode.updateItems(items);
        NodeRemote remoteSuccessor = RemoteUtil.getRemoteNode(fromNode.getHost(), fromNode.getId());
        if (remoteSuccessor == null) {
            logger.warn("Cannot get remote nodeId=" + fromNode.getId());
            return;
        }
        remoteSuccessor.removeItems(items);
        logger.debug("Transferred items fromNode=" + fromNode + " node=" + toNode + " items=" + Arrays.toString(items.toArray()));
    }

    /**
     * Returns a list of items, that should be transferred from successor node to current node
     * <p/>
     * If new node appears the last and successor it the first (zero-crossing)
     * it transfer only items between current node and its predecessor
     *
     * @param fromNode from which to transfer
     * @param toNode   from which to transfer
     * @return list of items for current node
     */
    private static List<Item> getTransferItems(Node fromNode, Node toNode) {
        boolean isZeroCrossed = fromNode.getId() < toNode.getId();
        List<Item> items = new ArrayList<>();
        for (Item item : fromNode.getItems().values()) {
            if (item.getKey() <= toNode.getId()) {
                // check if item (e.g. 5) falls in range of highest-identified node (e.g.20) or lowest (e.g. 5)
                if (isZeroCrossed) {
                    if (item.getKey() > fromNode.getId()) {
                        items.add(item);
                    }
                } else {
                    items.add(item);
                }
            } else {
                return items;
            }
        }
        return items;
    }

    public static void replicate(Node node) throws RemoteException {
        logger.debug("Replicating N=" + Replication.N + " for node=" + node);
        int nodeId = node.getId();
        for (int i = 0; i < Replication.N - 1; i++) {
            Node successorNode = getSuccessorNode(nodeId, node.getNodes());
            if (successorNode == null || successorNode.getId() == node.getId()) {
                break;
            }
            logger.debug("Replicating i=" + i + " to node=" + successorNode);
            copyItems(node, successorNode);
            nodeId = successorNode.getId();
        }
    }

    public static void replicate(Node node, final Item item) throws RemoteException {
        logger.debug("Replicating N=" + Replication.N + " for node=" + node);
        int nodeId = node.getId();
        for (int i = 0; i < Replication.N - 1; i++) {
            Node successorNode = getSuccessorNode(nodeId, node.getNodes());
            if (successorNode == null || successorNode.getId() == node.getId()) {
                break;
            }
            logger.debug("Replicating i=" + i + " to node=" + successorNode);
            copyItems(node, new ArrayList<Item>() {{
                add(item);
            }}, successorNode);
            nodeId = successorNode.getId();
        }
    }

    /**
     * Returns RMI string of the node for given host
     *
     * @param host network host
     * @return default lookup string
     */
    public static String getRMI(String host, int nodeId) {
        return "rmi://" + host + "/NodeRemote" + nodeId;
    }
}
