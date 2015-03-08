package it.unitn.ds.util;

import it.unitn.ds.entity.Node;
import it.unitn.ds.rmi.NodeServer;
import it.unitn.ds.rmi.NullNodeRemote;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;

import java.rmi.Naming;
import java.rmi.RemoteException;
import java.util.Collections;
import java.util.LinkedList;
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
    @NotNull
    public static <T> T getRemoteNode(@NotNull Node node, @NotNull Class<T> clazz) {
        try {
            return clazz.cast(Naming.lookup(getNodeRMI(node)));
        } catch (Exception e) {
            logger.error("Failed to get remote interface for id=" + node.getId(), e);
            try {
                return clazz.cast(new NullNodeRemote(new Node()));
            } catch (RemoteException re) {
                logger.error("Failed to get Null Node Pattern", re);
                throw new RuntimeException("RMI failed miserably", re);
            }
        }
    }

    /**
     * Returns RMI string of the remote node
     *
     * @param node remote node
     * @return default lookup string
     */
    @NotNull
    public static String getNodeRMI(@NotNull Node node) {
        return "rmi://" + node.getHost() + "/NodeRemote" + node.getId();
    }

    /**
     * Get node, responsible for item
     *
     * @param itemKey of the item
     * @param nodes   set of nodes
     * @return responsible node
     */
    @NotNull
    public static Node getNodeForItem(int itemKey, @NotNull Map<Integer, String> nodes) throws RemoteException {
        int nodeIdForItem = getNodeIdForItem(itemKey, nodes);
        logger.trace("Found NodeIdForItem=" + nodeIdForItem + " for itemKey=" + itemKey);
        return getRemoteNode(new Node(nodeIdForItem, nodes.get(nodeIdForItem)), NodeServer.class).getNode();
    }

    /**
     * Get node id, responsible for item
     *
     * @param itemKey of the item
     * @param nodes   set of nodes
     * @return responsible node id
     */
    public static int getNodeIdForItem(int itemKey, @NotNull Map<Integer, String> nodes) {
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
     * @param node  current node
     * @param nodes set of nodes
     * @return clockwise successor node
     */
    @NotNull
    public static Node getSuccessorNode(@NotNull Node node, @NotNull Map<Integer, String> nodes) throws RemoteException {
        int successorNodeId = getSuccessorNodeId(node.getId(), nodes);
        logger.trace("NodeId=" + node.getId() + " found successorNodeId=" + successorNodeId);
        return getNode(node, successorNodeId, nodes);
    }

    /**
     * Returns clockwise successor node id in the ring
     *
     * @param currentNodeId of the current node
     * @param nodes         set of nodes
     * @return clockwise successor node id
     */
    public static int getSuccessorNodeId(int currentNodeId, @NotNull Map<Integer, String> nodes) {
        for (int nodeId : nodes.keySet()) {
            if (nodeId > currentNodeId) {
                return nodeId;
            }
        }
        return nodes.keySet().iterator().next();
    }

    /**
     * Returns Nth successor node id in the ring
     *
     * @param node  current node
     * @param nodes set of nodes
     * @param count how many nodes to skip
     * @return nth successor node
     */
    @NotNull
    public static Node getNthSuccessor(@NotNull Node node, @NotNull Map<Integer, String> nodes, int count) throws RemoteException {
        int nodeId = node.getId();
        for (int i = 0; i < count; i++) {
            nodeId = getSuccessorNodeId(nodeId, nodes);
        }
        logger.trace("NodeId=" + node.getId() + " found nthSuccessor=" + nodeId);
        return getRemoteNode(new Node(nodeId, nodes.get(nodeId)), NodeServer.class).getNode();
    }

    /**
     * Returns counter clockwise predecessor node in the ring
     *
     * @param node  current node
     * @param nodes set of nodes
     * @return counter clockwise predecessor
     */
    @NotNull
    public static Node getPredecessorNode(@NotNull Node node, @NotNull Map<Integer, String> nodes) throws RemoteException {
        int predecessorNodeId = getPredecessorNodeId(node, nodes);
        logger.trace("NodeId=" + node.getId() + " found predecessorNodeId=" + predecessorNodeId);
        return getNode(node, predecessorNodeId, nodes);
    }

    /**
     * Gets node given nodeId
     *
     * @param currentNode current node
     * @param nodeId      of requested node
     * @param nodes       set of nodes
     * @return currentNode if nodeId is the same, remote node otherwise
     */
    @NotNull
    private static Node getNode(@NotNull Node currentNode, int nodeId, @NotNull Map<Integer, String> nodes) throws RemoteException {
        if (nodeId == currentNode.getId()) {
            return currentNode;
        } else {
            return getRemoteNode(new Node(nodeId, nodes.get(nodeId)), NodeServer.class).getNode();
        }
    }

    /**
     * Returns counter clockwise predecessor node id in the ring
     *
     * @param node  current node
     * @param nodes set of nodes
     * @return counterclockwise predecessor node
     */
    public static int getPredecessorNodeId(@NotNull Node node, @NotNull Map<Integer, String> nodes) {
        List<Integer> reverse = new LinkedList<>(nodes.keySet());
        Collections.reverse(reverse);
        for (int nodeId : reverse) {
            if (nodeId < node.getId()) {
                return nodeId;
            }
        }
        return reverse.iterator().next();
    }
}
