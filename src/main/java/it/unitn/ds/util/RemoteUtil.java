package it.unitn.ds.util;

import it.unitn.ds.server.NodeRemote;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.Nullable;

import java.rmi.Naming;
import java.util.TreeSet;

/**
 * Convenient class to deal with RMI for nodes
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
    public static int getNodeIdForItemKey(int key, TreeSet<Integer> nodes) {
        for (int nodeId : nodes) {
            if (nodeId >= key) {
                return nodeId;
            }
        }
        return nodes.iterator().next();
    }
}
