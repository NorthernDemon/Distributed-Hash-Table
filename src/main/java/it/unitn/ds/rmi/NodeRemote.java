package it.unitn.ds.rmi;

import it.unitn.ds.entity.Item;
import it.unitn.ds.entity.Node;
import it.unitn.ds.util.RemoteUtil;
import it.unitn.ds.util.StorageUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.Nullable;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public final class NodeRemote extends UnicastRemoteObject implements NodeServer, NodeClient {

    private static final Logger logger = LogManager.getLogger();

    private final Node node;

    public NodeRemote(Node node) throws RemoteException {
        this.node = node;
    }

    @Override
    public Node getNode() throws RemoteException {
        logger.debug("Get node=" + node);
        return node;
    }

    @Override
    public Map<Integer, String> getNodes() throws RemoteException {
        logger.debug("Get nodes=" + Arrays.toString(node.getNodes().entrySet().toArray()));
        return node.getNodes();
    }

    @Override
    public void addNode(int id, String host) throws RemoteException {
        logger.debug("Add id=" + id + ", host=" + host);
        node.putNode(id, host);
        logger.debug("Current nodes=" + Arrays.toString(node.getNodes().entrySet().toArray()));
    }

    @Override
    public void removeNode(int id) throws RemoteException {
        logger.debug("Remove id=" + id);
        node.removeNode(id);
        logger.debug("Current nodes=" + Arrays.toString(node.getNodes().entrySet().toArray()));
    }

    @Override
    public void updateItems(List<Item> items) throws RemoteException {
        logger.debug("Update items=" + Arrays.toString(items.toArray()));
        node.putItems(items);
        StorageUtil.write(node);
        logger.debug("Current items=" + Arrays.toString(node.getItems().keySet().toArray()));
    }

    @Override
    public void removeItems(List<Item> items) throws RemoteException {
        logger.debug("Remove items=" + Arrays.toString(items.toArray()));
        node.removeItems(items);
        StorageUtil.write(node);
        logger.debug("Current items=" + Arrays.toString(node.getItems().keySet().toArray()));
    }

    @Override
    public void updateReplicas(List<Item> replicas) throws RemoteException {
        logger.debug("Update replicas=" + Arrays.toString(replicas.toArray()));
        node.putReplicas(replicas);
        StorageUtil.write(node);
        logger.debug("Current replicas=" + Arrays.toString(node.getReplicas().keySet().toArray()));
    }

    @Override
    public void removeReplicas(List<Item> replicas) throws RemoteException {
        logger.debug("Remove replicas=" + Arrays.toString(replicas.toArray()));
        node.removeReplicas(replicas);
        StorageUtil.write(node);
        logger.debug("Current replicas=" + Arrays.toString(node.getReplicas().keySet().toArray()));
    }

    @Nullable
    @Override
    public Item getItem(int key) throws RemoteException {
        logger.debug("Get replica item with key=" + key);
        Item item = RemoteUtil.getLatestVersionItem(node.getNodes(), key);
        logger.debug("Got replica item=" + item);
        return item;
    }

    @Nullable
    @Override
    public Item updateItem(int key, String value) throws RemoteException {
        logger.debug("Update replica item with key=" + key + ", value=" + value);
        Item item = RemoteUtil.updateReplicas(node.getNodes(), key, value);
        logger.debug("Updated replica item=" + item);
        return item;
    }
}
