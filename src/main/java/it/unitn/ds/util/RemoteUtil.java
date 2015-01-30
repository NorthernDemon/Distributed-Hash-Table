package it.unitn.ds.util;

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
import java.util.TreeSet;

/**
 * Convenient class to deal with RMI
 */
public abstract class RemoteUtil {

    private static final Logger logger = LogManager.getLogger();

    public static final String RMI_NODE = "rmi://localhost/NodeRemote";

    /**
     * Get reference to remote node given node id
     *
     * @param nodeId of the wanted node
     * @return reference to remote object
     */
    @Nullable
    public static NodeRemote getRemoteNode(int nodeId) {
        try {
            return ((NodeRemote) Naming.lookup(RMI_NODE + nodeId));
        } catch (Exception e) {
            logger.error("Failed to get remote node by nodeId=" + nodeId);
            return null;
        }
    }

    /**
     * Returns successor node clockwise in circle of the given node id in the set of nodes
     *
     * @param nodeId predecessor node id
     * @param nodes  set of possible successors
     * @return successor node of the given node id
     */
    @Nullable
    public static Node getSuccessorNode(int nodeId, TreeSet<Integer> nodes) throws RemoteException {
        logger.debug("NodeId=" + nodeId + " is searching for successorNode...");
        int successorNodeId = getSuccessorNodeId(nodeId, nodes);
        if (successorNodeId == nodeId) {
            logger.debug("NodeId=" + nodeId + " did not find successorNode, except itself");
            return null;
        }
        logger.debug("NodeId=" + nodeId + " found successorNodeId=" + successorNodeId);
        return getRemoteNode(successorNodeId).getNode();
    }

    private static int getSuccessorNodeId(int targetNodeId, TreeSet<Integer> nodes) {
        for (int nodeId : nodes) {
            if (nodeId > targetNodeId) {
                return nodeId;
            }
        }
        return nodes.iterator().next();
    }

    /**
     * Copies items from one node to another, keeps items of fromNode
     *
     * @param fromNode from which to transfer
     * @param toNode   to which to transfer
     */
    public static void copyItems(Node fromNode, Node toNode) throws RemoteException {
        if (fromNode.getItems().isEmpty()) {
            logger.debug("Nothing to copy fromNode=" + fromNode + " toNode=" + toNode);
            return;
        }
        ArrayList<Item> items = new ArrayList<>(fromNode.getItems().values());
        logger.debug("Coping items fromNode=" + fromNode + " toNode=" + toNode + " items=" + Arrays.toString(items.toArray()));
        getRemoteNode(toNode.getId()).updateItems(items);
    }

    /**
     * Transfers items from one node to another, removes items of fromNode
     *
     * @param fromNode from which to transfer
     * @param toNode   to which to transfer
     */
    public static void transferItems(Node fromNode, Node toNode) throws RemoteException {
        if (fromNode.getItems().isEmpty()) {
            logger.debug("Nothing to transfer fromNode=" + fromNode + " toNode=" + toNode);
            return;
        }
        List<Item> items = getTransferItems(fromNode, toNode);
        logger.debug("Transferring items fromNode=" + fromNode + " toNode=" + toNode + " items=" + Arrays.toString(items.toArray()));
        getRemoteNode(toNode.getId()).updateItems(items);
        getRemoteNode(fromNode.getId()).removeItems(items);
    }

    /**
     * Returns a list of items, that should be transferred contained in toNode
     *
     * @param fromNode from which to transfer
     * @param toNode   to which to transfer
     * @return list of items for toNode
     */
    private static List<Item> getTransferItems(Node fromNode, Node toNode) {
        List<Item> items = new ArrayList<>();
        for (Item item : fromNode.getItems().values()) {
            if (item.getKey() <= toNode.getId()) {
                items.add(item);
            } else {
                return items;
            }
        }
        return items;
    }

    /**
     * Announce JOIN operation of the given node to the set of nodes
     *
     * @param node  to join
     * @param nodes announcement receivers
     */
    public static void announceJoin(Node node, TreeSet<Integer> nodes) throws RemoteException {
        logger.debug("NodeId=" + node.getId() + " announcing join to node.size()=" + nodes.size());
        for (int nodeId : nodes) {
            if (nodeId != node.getId()) {
                node.getNodes().add(nodeId);
                getRemoteNode(nodeId).addNode(node.getId());
                logger.debug("NodeId=" + node.getId() + " announced join to nodeId=" + nodeId);
            }
        }
    }

    /**
     * Announce LEAVE operation of the given node to the set of nodes
     *
     * @param node  to leave
     * @param nodes announcement receivers
     */
    public static void announceLeave(Node node, TreeSet<Integer> nodes) throws RemoteException {
        logger.debug("NodeId=" + node.getId() + " announcing leave to node.size()=" + nodes.size());
        for (int nodeId : nodes) {
            if (nodeId != node.getId()) {
                getRemoteNode(nodeId).removeNode(node.getId());
                logger.debug("NodeId=" + node.getId() + " announced leave to nodeId=" + nodeId);
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
    public static int getNodeIdForItemKey(int key, TreeSet<Integer> nodes) {
        for (int nodeId : nodes) {
            if (nodeId >= key) {
                return nodeId;
            }
        }
        return nodes.iterator().next();
    }
}
