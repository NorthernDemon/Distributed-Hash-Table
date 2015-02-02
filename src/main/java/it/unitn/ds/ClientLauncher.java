package it.unitn.ds;

import it.unitn.ds.server.Item;
import it.unitn.ds.util.InputUtil;
import it.unitn.ds.util.RemoteUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public final class ClientLauncher {

    private static final Logger logger = LogManager.getLogger();

    /**
     * ./client.jar {methodName},{host},{Node ID},{key},{value}
     * <p/>
     * Example: get,localhost,10,12
     * Example: update,localhost,15,12,New Value Item
     * Example: view,localhost,10
     *
     * @param args
     */
    public static void main(String args[]) {
        logger.info("Client is ready for request>>");
        logger.info("Example: {methodName},{host},{Node ID},{key},{value}");
        logger.info("Example: get,localhost,10,12");
        logger.info("Example: update,localhost,15,12,New Value Item");
        logger.info("Example: view,localhost,10");
        InputUtil.readInput(ClientLauncher.class.getName());
    }

    /**
     * Get item given node id and item key
     *
     * @param nodeId of the known node, does not have to contain item key
     * @param key    of the item
     */
    public static void get(String host, int nodeId, int key) {
        try {
            Item item = RemoteUtil.getRemoteNode(host, nodeId).getItem(key);
            logger.info("Got item=" + item + " from nodeId=" + nodeId);
        } catch (Exception e) {
            logger.error("RMI failed miserably", e);
            System.exit(1);
        }
    }

    /**
     * Update item given node id and item key
     *
     * @param nodeId of the known node, does not have to contain item key
     * @param key    of the item
     * @param value  new item value
     */
    public static void update(String host, int nodeId, int key, String value) {
        try {
            Item item = RemoteUtil.getRemoteNode(host, nodeId).updateItem(key, value);
            logger.info("Updated item=" + item + " from nodeId=" + nodeId);
        } catch (Exception e) {
            logger.error("RMI failed miserably", e);
            System.exit(1);
        }
    }

    /**
     * View topology of the circle from the given node id
     *
     * @param targetNodeId of the known node
     */
    public static void view(String host, int targetNodeId) {
        try {
            NodeRemote remoteNode = RemoteUtil.getRemoteNode(host, targetNodeId);
            if (remoteNode == null) {
                logger.warn("Cannot get remote nodeId=" + targetNodeId);
                return;
            }
            for (Map.Entry<Integer, String> entry : remoteNode.getNodes().entrySet()) {
                NodeRemote node = RemoteUtil.getRemoteNode(entry.getValue(), entry.getKey());
                if (node == null) {
                    logger.warn("Cannot get remote nodeId=" + entry.getKey());
                } else {
                    logger.debug("Node=" + node.getNode());
                }
            }
            logger.info("Viewed topology from targetNodeId=" + targetNodeId);
        } catch (Exception e) {
            logger.error("RMI failed miserably", e);
            System.exit(1);
        }
    }
}
