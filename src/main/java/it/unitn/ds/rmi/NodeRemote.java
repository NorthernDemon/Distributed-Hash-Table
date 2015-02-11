package it.unitn.ds.rmi;

import it.unitn.ds.Replication;
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
import java.util.Iterator;
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
    public void addNode(int nodeId, String host) throws RemoteException {
        logger.debug("Add node=" + nodeId + ", host=" + host);
        node.putNode(nodeId, host);
        logger.debug("Current nodes=" + Arrays.toString(node.getNodes().entrySet().toArray()));
    }

    @Override
    public void removeNode(int nodeId) throws RemoteException {
        logger.debug("Remove node=" + nodeId);
        node.removeNode(nodeId);
        logger.debug("Current nodes=" + Arrays.toString(node.getNodes().entrySet().toArray()));
    }

    @Override
    public void updateItems(List<Item> items) throws RemoteException {
        logger.debug("Update items=" + Arrays.toString(items.toArray()));
        node.putItems(items);
        StorageUtil.write(node);
        logger.debug("Current items=" + Arrays.toString(node.getItems().values().toArray()));
    }

    @Override
    public void removeItems(List<Item> items) throws RemoteException {
        logger.debug("Remove items=" + Arrays.toString(items.toArray()));
        node.removeItems(items);
        StorageUtil.write(node);
        logger.debug("Current items=" + Arrays.toString(node.getItems().values().toArray()));
    }

    @Nullable
    @Override
    public Item getItem(int key) throws RemoteException {
        Item item = getLatestVersionItem(RemoteUtil.getReplicas(node.getNodes(), key));
        logger.debug("Got item=" + item);
        return item;
    }

    @Nullable
    @Override
    public Item updateItem(int key, String value) throws RemoteException {
        List<Item> replicas = RemoteUtil.getReplicas(node.getNodes(), key);
        if (!replicas.isEmpty() && replicas.size() != Math.max(Replication.R, Replication.W)) {
            logger.debug("No can agree on WRITE quorum: Q != max(R,W) as Q=" + replicas.size() + ", R=" + Replication.R + ", W=" + Replication.W);
            return null;
        }
        Item item = new Item(key, value, incrementLatestVersion(replicas));
        RemoteUtil.updateReplicas(node.getNodes(), item);
        return item;
    }

    @Nullable
    private Item getLatestVersionItem(List<Item> replicas) throws RemoteException {
        if (replicas.isEmpty()) {
            return null;
        }
        Iterator<Item> iterator = replicas.iterator();
        Item item = iterator.next();
        while (iterator.hasNext()) {
            Item replica = iterator.next();
            if (replica.getVersion() > item.getVersion()) {
                item = replica;
            }
        }
        return item;
    }

    private int incrementLatestVersion(List<Item> replicas) throws RemoteException {
        int version = 1;
        Item replica = getLatestVersionItem(replicas);
        if (replica != null) {
            version += replica.getVersion();
        }
        return version;
    }
}
