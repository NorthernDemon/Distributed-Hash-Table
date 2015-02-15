package it.unitn.ds.util;

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
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Convenient class to deal with RMI for nodes
 */
public abstract class RemoteUtil {

    private static final Logger logger = LogManager.getLogger();

    /**
     * Get reference to remote node
     *
     * @param node  remote node
     * @param clazz type of the interface
     * @return reference to remote object
     * @see it.unitn.ds.rmi.NodeClient
     * @see it.unitn.ds.rmi.NodeServer
     */
    public static <T> T getRemoteNode(Node node, Class<T> clazz) {
        try {
            return clazz.cast(Naming.lookup(getNodeRMI(node)));
        } catch (Exception e) {
            logger.error("Failed to get Remote Interface for id=" + node.getId(), e);
            try {
                return clazz.cast(new NullNodeRemote(new Node()));
            } catch (RemoteException re) {
                logger.error("Failed to get Null Node Pattern", re);
                return null;
            }
        }
    }

    /**
     * Returns RMI string of the remote node
     *
     * @param node remote node
     * @return default lookup string
     */
    public static String getNodeRMI(Node node) {
        return "rmi://" + node.getHost() + "/NodeRemote" + node.getId();
    }

    /**
     * Get node, responsible for item key
     *
     * @param itemKey of the item
     * @param nodes   set of nodes
     * @return responsible node
     */
    public static Node getNodeForItem(int itemKey, Map<Integer, String> nodes) throws RemoteException {
        int nodeIdForItem = getNodeIdForItem(itemKey, nodes);
        return getRemoteNode(new Node(nodeIdForItem, nodes.get(nodeIdForItem)), NodeServer.class).getNode();
    }

    /**
     * Get node id, responsible for item key
     *
     * @param itemKey of the item
     * @param nodes   set of nodes
     * @return responsible node id
     */
    private static int getNodeIdForItem(int itemKey, Map<Integer, String> nodes) {
        for (int nodeId : nodes.keySet()) {
            if (nodeId >= itemKey) {
                return nodeId;
            }
        }
        return nodes.keySet().iterator().next();
    }

    /**
     * Returns clockwise successor node in the ring
     *
     * @param nodeId of the current node
     * @param nodes  set of nodes
     * @return clockwise successor node
     */
    @Nullable
    public static Node getSuccessorNode(int nodeId, Map<Integer, String> nodes) throws RemoteException {
        int successorNodeId = getSuccessorNodeId(nodeId, nodes);
        if (successorNodeId == nodeId) {
            logger.info("NodeId=" + nodeId + " is a successor to itself!");
            return null;
        }
        logger.debug("NodeId=" + nodeId + " found successorNodeId=" + successorNodeId);
        return getRemoteNode(new Node(successorNodeId, nodes.get(successorNodeId)), NodeServer.class).getNode();
    }

    /**
     * Returns Nth successor
     *
     * @param node  current node
     * @param count how many nodes to skip
     * @return nth successor node
     */
    public static Node getNthSuccessor(Node node, int count) throws RemoteException {
        int nodeId = node.getId();
        for (int i = 0; i < count; i++) {
            nodeId = getSuccessorNodeId(nodeId, node.getNodes());
        }
        return getRemoteNode(new Node(nodeId, node.getNodes().get(nodeId)), NodeServer.class).getNode();
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
     * Returns a list of items, that the node is responsible for
     *
     * @param node          current node
     * @param successorNode of the current node
     * @return list of items, responsible of
     */
    public static List<Item> getNodeItems(Node node, Node successorNode) {
        int predecessorNodeId = RemoteUtil.getPredecessorNodeId(node);
        boolean isZeroCrossed = node.getId() < predecessorNodeId;
        List<Item> items = new ArrayList<>();
        for (Item item : successorNode.getItems().values()) {
            // check if item (e.g. 5) falls in range of highest-identified node (e.g. 20) or lowest (e.g. 5)
            if (isZeroCrossed && (item.getKey() <= node.getId() || item.getKey() > successorNode.getId())) {
                items.add(item);
            }
            if (!isZeroCrossed && item.getKey() <= node.getId() && item.getKey() > predecessorNodeId) {
                items.add(item);
            }
        }
        return items;
    }

    /**
     * Returns predecessor node id
     *
     * @param node current node
     * @return predecessor node
     */
    private static int getPredecessorNodeId(Node node) {
        List<Integer> reverse = new ArrayList<>(node.getNodes().keySet());
        Collections.reverse(reverse);
        for (int nodeId : reverse) {
            if (nodeId < node.getId()) {
                return nodeId;
            }
        }
        return reverse.iterator().next();
    }
}
