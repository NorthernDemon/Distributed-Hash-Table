package it.unitn.ds.server;

import it.unitn.ds.util.RemoteUtil;
import it.unitn.ds.util.StorageUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public final class NodeRemoteImpl extends UnicastRemoteObject implements NodeRemote {

    private static final Logger logger = LogManager.getLogger();

    private Node node;

    public NodeRemoteImpl(Node node) throws RemoteException {
        this.node = node;
    }

    @Override
    public Node getNode() throws RemoteException {
        logger.debug("Get node=" + node);
        return node;
    }

    @Override
    public Map<Integer, String> getNodes() throws RemoteException {
        logger.debug("Get hosts=" + Arrays.toString(node.getNodes().entrySet().toArray()));
        return node.getNodes();
    }

    @Override
    public void addNode(int nodeId, String host) throws RemoteException {
        logger.debug("Add node request with node=" + nodeId);
        node.getNodes().put(nodeId, host);
        logger.debug("Current nodes=" + Arrays.toString(node.getNodes().entrySet().toArray()));
    }

    @Override
    public void removeNode(int nodeId) throws RemoteException {
        logger.debug("Remove node request with node=" + nodeId);
        node.getNodes().remove(nodeId);
        logger.debug("Current nodes=" + Arrays.toString(node.getNodes().entrySet().toArray()));
    }

    @Override
    public void updateItems(List<Item> items) throws RemoteException {
        logger.debug("Update items request with items=" + Arrays.toString(items.toArray()));
        for (Item item : items) {
            node.getItems().put(item.getKey(), item);
        }
        StorageUtil.write(node);
        logger.debug("Current items=" + Arrays.toString(node.getItems().values().toArray()));
    }

    @Override
    public void removeItems(List<Item> items) throws RemoteException {
        logger.debug("Remove items request with items=" + Arrays.toString(items.toArray()));
        for (Item item : items) {
            node.getItems().remove(item.getKey());
        }
        StorageUtil.write(node);
        logger.debug("Current items=" + Arrays.toString(node.getItems().values().toArray()));
    }

    @Override
    public Item getItem(int key) throws RemoteException {
        logger.debug("Get item request with key=" + key);
        int nodeId = RemoteUtil.getNodeIdForItemKey(key, node.getNodes());
        if (nodeId == node.getId()) {
            Item item = node.getItems().get(key);
            logger.debug("Got item=" + item);
            return item;
        } else {
            logger.debug("Forwarding GET item request to nodeId=" + nodeId);
            return RemoteUtil.getRemoteNode(node.getNodes().get(nodeId), nodeId).getItem(key);
        }
    }

    @Override
    public Item updateItem(int key, String value) throws RemoteException {
        logger.debug("Update item request with key=" + key);
        int nodeId = RemoteUtil.getNodeIdForItemKey(key, node.getNodes());
        if (nodeId == node.getId()) {
            Item item = new Item(key, value, 0);
            node.getItems().put(item.getKey(), item);
            StorageUtil.write(node);
            logger.debug("Updated item=" + item);
            return item;
        } else {
            logger.debug("Forwarding UPDATE item request to nodeId=" + nodeId);
            return RemoteUtil.getRemoteNode(node.getNodes().get(nodeId), nodeId).updateItem(key, value);
        }
    }
}
