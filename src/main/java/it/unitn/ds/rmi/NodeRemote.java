package it.unitn.ds.rmi;

import it.unitn.ds.Replication;
import it.unitn.ds.entity.Item;
import it.unitn.ds.entity.Node;
import it.unitn.ds.util.MultithreadingUtil;
import it.unitn.ds.util.RemoteUtil;
import it.unitn.ds.util.StorageUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Provides an access to remote node via RMI
 */
public final class NodeRemote extends UnicastRemoteObject implements NodeServer, NodeClient {

    private static final Logger logger = LogManager.getLogger();

    @NotNull
    private final Node node;

    public NodeRemote(@NotNull Node node) throws RemoteException {
        this.node = node;
    }

    @NotNull
    @Override
    public Node getNode() throws RemoteException {
        logger.debug("Get node=" + node);
        return node;
    }

    @NotNull
    @Override
    public Map<Integer, String> getNodes() throws RemoteException {
        logger.debug("Get nodes=" + Arrays.toString(node.getNodes().entrySet().toArray()));
        return node.getNodes();
    }

    @Override
    public void addNode(int id, @NotNull String host) throws RemoteException {
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
    public void updateItems(@NotNull List<Item> items) throws RemoteException {
        logger.debug("Update items=" + Arrays.toString(items.toArray()));
        node.putItems(items);
        StorageUtil.write(node);
        logger.debug("Current items=" + Arrays.toString(node.getItems().keySet().toArray()));
    }

    @Override
    public void removeItems(@NotNull List<Item> items) throws RemoteException {
        logger.debug("Remove items=" + Arrays.toString(items.toArray()));
        node.removeItems(items);
        StorageUtil.write(node);
        logger.debug("Current items=" + Arrays.toString(node.getItems().keySet().toArray()));
    }

    @Override
    public void updateReplicas(@NotNull List<Item> replicas) throws RemoteException {
        logger.debug("Update replicas=" + Arrays.toString(replicas.toArray()));
        node.putReplicas(replicas);
        StorageUtil.write(node);
        logger.debug("Current replicas=" + Arrays.toString(node.getReplicas().keySet().toArray()));
    }

    @Override
    public void removeReplicas(@NotNull List<Item> replicas) throws RemoteException {
        logger.debug("Remove replicas=" + Arrays.toString(replicas.toArray()));
        node.removeReplicas(replicas);
        StorageUtil.write(node);
        logger.debug("Current replicas=" + Arrays.toString(node.getReplicas().keySet().toArray()));
    }

    @Nullable
    @Override
    public Item getItem(int key) throws RemoteException {
        logger.debug("Get replica item with key=" + key);
        Item item = getLatestVersion(getReplicas(key));
        logger.debug("Got replica item=" + item);
        return item;
    }

    @Nullable
    @Override
    public Item updateItem(int key, @NotNull String value) throws RemoteException {
        logger.debug("Update replica item with key=" + key + ", value=" + value);
        Item item = updateReplicas(key, value);
        logger.debug("Updated replica item=" + item);
        return item;
    }

    @NotNull
    private List<Item> getReplicas(int itemKey) throws RemoteException {
        Node nodeForItem = RemoteUtil.getNodeForItem(itemKey, node.getNodes());
        Item item = nodeForItem.getItems().get(itemKey);
        List<Item> replicas = MultithreadingUtil.getReplicas(itemKey, nodeForItem, item == null ? 0 : 1, node.getNodes());
        if (item != null) {
            logger.debug("Got original item=" + item + " from nodeForItem=" + nodeForItem);
            replicas.add(item);
        }
        return replicas;
    }

    @Nullable
    private Item updateReplicas(int itemKey, @NotNull String itemValue) throws RemoteException {
        List<Item> replicas = getReplicas(itemKey);
        if (!replicas.isEmpty() && replicas.size() != Math.max(Replication.R, Replication.W)) {
            logger.debug("No can agree on WRITE quorum: Q != max(R,W) as Q=" + replicas.size() + ", R=" + Replication.R + ", W=" + Replication.W);
            return null;
        }
        Item item = createOrUpdate(itemKey, itemValue, replicas);
        Node nodeForItem = RemoteUtil.getNodeForItem(itemKey, node.getNodes());
        RemoteUtil.getRemoteNode(nodeForItem, NodeServer.class).updateItems(Arrays.asList(item));
        logger.debug("Updated item=" + item + " to nodeForItem=" + nodeForItem);
        MultithreadingUtil.updateReplicas(item, nodeForItem, node.getNodes());
        return item;
    }

    @NotNull
    private Item createOrUpdate(int itemKey, @NotNull String itemValue, @NotNull List<Item> replicas) throws RemoteException {
        Item item = getLatestVersion(replicas);
        if (item == null) {
            return new Item(itemKey, itemValue);
        } else {
            item.update(itemValue);
            return item;
        }
    }

    @Nullable
    private Item getLatestVersion(@NotNull List<Item> replicas) throws RemoteException {
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
