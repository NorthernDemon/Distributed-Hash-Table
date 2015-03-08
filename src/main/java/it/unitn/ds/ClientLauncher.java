package it.unitn.ds;

import it.unitn.ds.entity.Item;
import it.unitn.ds.entity.Node;
import it.unitn.ds.rmi.NodeClient;
import it.unitn.ds.util.InputUtil;
import it.unitn.ds.util.NetworkUtil;
import it.unitn.ds.util.RemoteUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;

import java.rmi.RemoteException;

/**
 * Simulates client of the server node's ring
 *
 * @see it.unitn.ds.ServerLauncher
 */
public final class ClientLauncher {

    private static final Logger logger = LogManager.getLogger();

    /**
     * Description: method name,node host,node id,item key,item value
     * Example: update,localhost,10,8,New Value Item
     * Example: update,localhost,10,12,New Value Item
     * Example: update,localhost,10,17,New Value Item
     * Example: update,localhost,10,22,New Value Item
     * Example: update,localhost,10,26,New Value Item
     * Example: get,localhost,10,12
     * Example: view,localhost,10
     */
    public static void main(String args[]) {
        logger.info("Type in: method name,node host,node id,item key,item value");
        logger.info("Example: update,localhost,10,8,New Value Item");
        logger.info("Example: update,localhost,10,12,New Value Item");
        logger.info("Example: update,localhost,10,17,New Value Item");
        logger.info("Example: update,localhost,10,22,New Value Item");
        logger.info("Example: update,localhost,10,26,New Value Item");
        logger.info("Example: get,localhost,10,12");
        NetworkUtil.printMachineIPv4();
        logger.info("Client is ready for request >");
        InputUtil.readInput(ClientLauncher.class.getName());
    }

    /**
     * Get item from the node in the ring
     *
     * @param coordinatorHost   of the node
     * @param coordinatorNodeId of the node, does not have to contain item
     * @param itemKey           of the item
     */
    public static void get(@NotNull String coordinatorHost, int coordinatorNodeId, int itemKey) throws RemoteException {
        Node coordinatorNode = new Node(coordinatorNodeId, coordinatorHost);
        Item item = RemoteUtil.getRemoteNode(coordinatorNode, NodeClient.class).getItem(itemKey);
        logger.info("Got item=" + item + " from coordinatorNodeId=" + coordinatorNodeId);
    }

    /**
     * Creates/Update item of the node in the ring
     *
     * @param coordinatorHost   of the node
     * @param coordinatorNodeId of the node, does not have to contain item
     * @param itemKey           of the item
     * @param itemValue         new item value
     */
    public static void update(@NotNull String coordinatorHost, int coordinatorNodeId, int itemKey, @NotNull String itemValue) throws RemoteException {
        if (itemKey <= 0) {
            logger.warn("Item key must be positive integer [ itemKey > 0 ] !");
            return;
        }
        Node coordinatorNode = new Node(coordinatorNodeId, coordinatorHost);
        Item item = RemoteUtil.getRemoteNode(coordinatorNode, NodeClient.class).updateItem(itemKey, itemValue);
        logger.info("Updated item=" + item + " from coordinatorNodeId=" + coordinatorNodeId);
    }
}
