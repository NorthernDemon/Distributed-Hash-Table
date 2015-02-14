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
import java.util.*;

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
        Item item = getItemReplica(key);
        logger.debug("Got replica item=" + item);
        return item;
    }

    @Nullable
    @Override
    public Item updateItem(int key, String value) throws RemoteException {
        logger.debug("Update replica item with key=" + key + ", value=" + value);
        Item item = updateReplicas(key, value);
        logger.debug("Updated replica item=" + item);
        return item;
    }

    private List<Item> getReplicas(int itemKey) throws RemoteException {
        Node originalNode = RemoteUtil.getNodeForItem(itemKey, node.getNodes());
        List<Item> items = new ArrayList<>(Replication.N);
        Item item = originalNode.getItems().get(itemKey);
        if (item != null) {
            items.add(item);
            logger.debug("Got original item=" + item + " from originalNode=" + originalNode);
        }
        for (int i = 1; i < Replication.N; i++) {
            Node nthSuccessor = RemoteUtil.getNthSuccessor(originalNode, node.getNodes(), i);
            if (items.size() != Replication.R) {
                item = nthSuccessor.getReplicas().get(itemKey);
                if (item != null) {
                    items.add(item);
                    logger.debug("Got replicas of item=" + item + " from nthSuccessor=" + nthSuccessor);
                }
            } else {
                return items;
            }
        }
        return items;
    }

    @Nullable
    private Item updateReplicas(int itemKey, String itemValue) throws RemoteException {
        List<Item> replicas = getReplicas(itemKey);
        if (!replicas.isEmpty() && replicas.size() != Math.max(Replication.R, Replication.W)) {
            logger.debug("No can agree on WRITE quorum: Q != max(R,W) as Q=" + replicas.size() + ", R=" + Replication.R + ", W=" + Replication.W);
            return null;
        }
        Node originalNode = RemoteUtil.getNodeForItem(itemKey, node.getNodes());
        Item item = new Item(itemKey, itemValue, incrementLatestVersion(replicas));
        RemoteUtil.getRemoteNode(originalNode, NodeServer.class).updateItems(Arrays.asList(item));
        logger.debug("Replicated original item=" + item + " to originalNode=" + originalNode);
        for (int i = 1; i < Replication.N; i++) {
            Node nthSuccessor = RemoteUtil.getNthSuccessor(originalNode, node.getNodes(), i);
            if (nthSuccessor != null && nthSuccessor.getId() != originalNode.getId()) {
                RemoteUtil.getRemoteNode(nthSuccessor, NodeServer.class).updateReplicas(Arrays.asList(item));
                logger.debug("Replicated item=" + item + " to nthSuccessor=" + nthSuccessor);
            }
        }
        return getItemReplica(itemKey);
    }

    private int incrementLatestVersion(List<Item> replicas) throws RemoteException {
        int version = 1;
        Item item = getLatestVersionItem(replicas);
        if (item != null) {
            version += item.getVersion();
        }
        return version;
    }

    private Item getItemReplica(int itemKey) throws RemoteException {
        return getLatestVersionItem(getReplicas(itemKey));
    }

    @Nullable
    private Item getLatestVersionItem(List<Item> replicas) throws RemoteException {
        Iterator<Item> iterator = replicas.iterator();
        if (iterator.hasNext()) {
            Item item = iterator.next();
            while (iterator.hasNext()) {
                Item replica = iterator.next();
                if (replica.getVersion() > item.getVersion()) {
                    item = replica;
                }
            }
            return item;
        }
        return null;
    }
}
