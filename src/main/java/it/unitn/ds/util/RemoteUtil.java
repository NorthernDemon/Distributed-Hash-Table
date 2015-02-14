package it.unitn.ds.util;

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
     * Get reference to remote node given the set of known nodes and node id
     *
     * @param nodes set of known nodes
     * @param id    node id
     * @param clazz type of the interface
     * @return reference to remote object
     */
    public static <T> T getRemoteNode(Map<Integer, String> nodes, int id, Class<T> clazz) {
        return getRemoteNode(nodes.get(id), id, clazz);
    }

    /**
     * Get reference to remote node given the node itself
     *
     * @param node  current node
     * @param clazz type of the interface
     * @return reference to remote object
     */
    public static <T> T getRemoteNode(Node node, Class<T> clazz) {
        if (node == null) {
            return getNullNodeRemote(clazz);
        }
        return getRemoteNode(node.getHost(), node.getId(), clazz);
    }

    /**
     * Get reference to remote node given teh node id and node host
     *
     * @param host  network host
     * @param id    of the wanted node
     * @param clazz type of the interface
     * @return reference to remote object
     */
    public static <T> T getRemoteNode(String host, int id, Class<T> clazz) {
        try {
            return clazz.cast(Naming.lookup(getNodeRMI(host, id)));
        } catch (Exception e) {
            logger.error("Failed to get Remote Interface for id=" + id, e);
            return getNullNodeRemote(clazz);
        }
    }

    /**
     * Returns RMI string of the node for given node
     *
     * @param node the current node
     * @return default lookup string
     */
    public static String getNodeRMI(Node node) {
        return getNodeRMI(node.getHost(), node.getId());
    }

    /**
     * Returns RMI string of the node for given host and id
     *
     * @param host network host
     * @param id   id of the current node
     * @return default lookup string
     */
    public static String getNodeRMI(String host, int id) {
        return "rmi://" + host + "/NodeRemote" + id;
    }

    @Nullable
    private static <T> T getNullNodeRemote(Class<T> clazz) {
        try {
            return clazz.cast(new NullNodeRemote(new Node()));
        } catch (RemoteException re) {
            logger.error("Failed to get Null Node Pattern", re);
            return null;
        }
    }

    /**
     * Get node for given item key in the set of nodes
     *
     * @param key   of the item
     * @param nodes set of known nodes
     * @return node, responsible for given item
     */
    public static Node getNodeForItem(int key, Map<Integer, String> nodes) throws RemoteException {
        return getRemoteNode(nodes, getNodeIdForItem(key, nodes), NodeServer.class).getNode();
    }

    /**
     * Get node id for given item key in the set of nodes
     *
     * @param key   of the item
     * @param nodes set of known nodes
     * @return node id
     */
    private static int getNodeIdForItem(int key, Map<Integer, String> nodes) {
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
        if (node == null) {
            return null;
        }
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
        return getRemoteNode(nodes, successorNodeId, NodeServer.class).getNode();
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
     * Returns predecessor node id of the given node
     *
     * @param node successor node
     * @return predecessor node
     */
    public static int getPredecessorNodeId(Node node) {
        List<Integer> reverse = new ArrayList<>(node.getNodes().keySet());
        Collections.reverse(reverse);
        for (int nodeId : reverse) {
            if (nodeId < node.getId()) {
                return nodeId;
            }
        }
        return reverse.iterator().next();
    }

    /**
     * Returns Nth successor of the given node
     *
     * @param node  current node
     * @param count how many nodes to skip
     * @param nodes set of known nodes
     * @return successor node
     * @throws RemoteException in case of RMI exception
     */
    public static Node getNthSuccessor(Node node, Map<Integer, String> nodes, int count) throws RemoteException {
        int nodeId = node.getId();
        for (int i = 0; i < count; i++) {
            nodeId = getSuccessorNodeId(nodeId, nodes);
        }
        return getRemoteNode(nodes, nodeId, NodeServer.class).getNode();
    }
}
