package it.unitn.ds;

import it.unitn.ds.entity.Item;
import it.unitn.ds.rmi.NodeClient;
import it.unitn.ds.rmi.NodeServer;
import it.unitn.ds.util.InputUtil;
import it.unitn.ds.util.RemoteUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.rmi.RemoteException;
import java.util.Map;

public final class ClientLauncher {

    private static final Logger logger = LogManager.getLogger();

    /**
     * Description: method name,node host,node id,item key,item value
     * Example: update,localhost,10,12,New Value Item
     * Example: get,localhost,10,12
     * Example: view,localhost,10
     */
    public static void main(String args[]) {
        logger.info("Client is ready for request >>");
        logger.info("Example: method name,node host,node id,item key,item value");
        logger.info("Example: update,localhost,10,12,New Value Item");
        logger.info("Example: get,localhost,10,12");
        logger.info("Example: view,localhost,10");
        InputUtil.readInput(ClientLauncher.class.getName());
    }

    /**
     * Get item given node id and item itemKey
     *
     * @param coordinatorHost   of the known node
     * @param coordinatorNodeId of the known node, does not have to contain item itemKey
     * @param itemKey           of the item
     */
    public static void get(String coordinatorHost, int coordinatorNodeId, int itemKey) throws RemoteException {
        Item item = RemoteUtil.getRemoteNode(coordinatorHost, coordinatorNodeId, NodeClient.class).getItem(itemKey);
        logger.info("Got item=" + item + " from coordinatorNodeId=" + coordinatorNodeId);
    }

    /**
     * Update item given node id and item itemKey
     *
     * @param coordinatorHost   of the known node
     * @param coordinatorNodeId of the known node, does not have to contain item itemKey
     * @param itemKey           of the item
     * @param itemValue         new item itemValue
     */
    public static void update(String coordinatorHost, int coordinatorNodeId, int itemKey, String itemValue) throws RemoteException {
        Item item = RemoteUtil.getRemoteNode(coordinatorHost, coordinatorNodeId, NodeClient.class).updateItem(itemKey, itemValue);
        logger.info("Updated item=" + item + " from coordinatorNodeId=" + coordinatorNodeId);
    }

    /**
     * View ring topology from the given node id
     *
     * @param coordinatorHost   of the known node
     * @param coordinatorNodeId of the known node
     */
    public static void view(String coordinatorHost, int coordinatorNodeId) throws RemoteException {
        Map<Integer, String> nodes = RemoteUtil.getRemoteNode(coordinatorHost, coordinatorNodeId, NodeServer.class).getNodes();
        for (Map.Entry<Integer, String> entry : nodes.entrySet()) {
            NodeServer nodeServer = RemoteUtil.getRemoteNode(entry.getValue(), entry.getKey(), NodeServer.class);
            logger.debug("Node=" + nodeServer.getNode());
        }
        logger.info("Viewed topology from coordinatorNodeId=" + coordinatorNodeId);
    }
}
