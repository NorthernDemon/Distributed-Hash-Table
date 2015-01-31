package it.unitn.ds;

import it.unitn.ds.server.Item;
import it.unitn.ds.util.InputUtil;
import it.unitn.ds.util.RemoteUtil;
import it.unitn.ds.util.StorageUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public final class ClientLauncher {

    private static final Logger logger = LogManager.getLogger();

    /**
     * ./client.jar {methodName},{Node ID},{key},{value}
     * <p/>
     * Example: get,10,12
     * Example: update,15,12,New Value Item
     *
     * @param args
     */
    public static void main(String args[]) {
        logger.info("Client is ready for request>>");
        logger.info("Example: {methodName},{Node ID},{key},{value}");
        logger.info("Example: get,10,12");
        logger.info("Example: update,15,12,New Value Item");
        InputUtil.readInput(ClientLauncher.class.getName());
    }

    /**
     * Get item given node id and item key
     *
     * @param nodeId of the known node, does not have to contain item key
     * @param key    of the item
     */
    public static void get(int nodeId, int key) {
        try {
            Item item = RemoteUtil.getRemoteNode(nodeId).getItem(key);
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
    public static void update(int nodeId, int key, String value) {
        if (value.contains(StorageUtil.SEPARATOR)) {
            logger.warn("Cannot store \"" + StorageUtil.SEPARATOR + "\" in value field... yet!");
            return;
        }
        try {
            Item item = RemoteUtil.getRemoteNode(nodeId).updateItem(key, value);
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
    public static void view(int targetNodeId) {
        try {
            for (int nodeId : RemoteUtil.getRemoteNode(targetNodeId).getNodes()) {
                logger.debug("Node=" + RemoteUtil.getRemoteNode(nodeId).getNode());
            }
            logger.info("Viewed topology from targetNodeId=" + targetNodeId);
        } catch (Exception e) {
            logger.error("RMI failed miserably", e);
            System.exit(1);
        }
    }
}
