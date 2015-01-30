package it.unitn.ds.server;

import it.unitn.ds.util.RemoteUtil;
import it.unitn.ds.util.StorageUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.Arrays;
import java.util.List;
import java.util.TreeSet;

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
    public TreeSet<Integer> getNodes() throws RemoteException {
        logger.debug("Get nodes=" + Arrays.toString(node.getNodes().toArray()));
        return node.getNodes();
    }

    @Override
    public void addNode(int nodeId) throws RemoteException {
        logger.debug("Add node request with node=" + nodeId);
        node.getNodes().add(nodeId);
        logger.debug("Current nodes=" + Arrays.toString(node.getNodes().toArray()));
    }

    @Override
    public void removeNode(int nodeId) throws RemoteException {
        logger.debug("Remove node request with node=" + nodeId);
        node.getNodes().remove(nodeId);
        logger.debug("Current nodes=" + Arrays.toString(node.getNodes().toArray()));
    }

    @Override
    public void updateItems(List<Item> items) throws RemoteException {
        logger.debug("Update items request with items=" + Arrays.toString(items.toArray()));
        StorageUtil.write(node, items);
        logger.debug("Current items=" + Arrays.toString(node.getItems().values().toArray()));
    }

    @Override
    public Item getItem(int key) throws RemoteException {
        logger.debug("Get item request with key=" + key);
        int nodeId = RemoteUtil.getNodeIdForItemKey(key, node.getNodes());
        if (nodeId == node.getId()) {
            logger.debug("I am the correct node for item");
            return node.getItems().get(key);
        } else {
            logger.debug("Forwarding get item request to nodeId=" + nodeId);
            return RemoteUtil.getRemoteNode(nodeId).getItem(key);
        }
    }

    @Override
    public Item updateItem(int key, String value) throws RemoteException {
        logger.debug("Update item request with key=" + key);
        int nodeId = RemoteUtil.getNodeIdForItemKey(key, node.getNodes());
        if (nodeId == node.getId()) {
            logger.debug("I am the correct node for item");
            return StorageUtil.write(node, new Item(key, value, 0));
        } else {
            logger.debug("Forwarding update item request to nodeId=" + nodeId);
            return RemoteUtil.getRemoteNode(nodeId).updateItem(key, value);
        }
    }
}
