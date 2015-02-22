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
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Provides an access to remote node via RMI
 * <p/>
 * Uses read/write locks for manipulation with internal data structure of the node in case of multiple requests
 * <p/>
 * Read Lock: multiple readers can enter, if not locked for writing
 * Write Lock: only one writer can enter, if not locked for reading
 *
 * @see it.unitn.ds.entity.Item
 * @see it.unitn.ds.entity.Node
 * @see java.util.concurrent.locks.ReadWriteLock
 * @see java.util.concurrent.locks.ReentrantReadWriteLock
 */
public final class NodeRemote extends UnicastRemoteObject implements NodeServer, NodeClient {

    private static final Logger logger = LogManager.getLogger();

    /**
     * Locks nodes TreeMap operations of the node
     */
    private static final ReadWriteLock nodesLock = new ReentrantReadWriteLock();

    /**
     * Locks items TreeMap operations of the node
     */
    private static final ReadWriteLock itemsLock = new ReentrantReadWriteLock();

    /**
     * Locks replication TreeMap operations of the node
     */
    private static final ReadWriteLock replicasLock = new ReentrantReadWriteLock();

    /**
     * Locks client operations of the node
     */
    private static final ReadWriteLock clientLock = new ReentrantReadWriteLock();

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
        nodesLock.readLock().lock();
        try {
            logger.debug("Get nodes=" + Arrays.toString(node.getNodes().entrySet().toArray()));
            return node.getNodes();
        } finally {
            nodesLock.readLock().unlock();
        }
    }

    @Override
    public void addNode(int id, @NotNull String host) throws RemoteException {
        nodesLock.writeLock().lock();
        try {
            logger.debug("Add id=" + id + ", host=" + host);
            node.putNode(id, host);
            logger.debug("Current nodes=" + Arrays.toString(node.getNodes().entrySet().toArray()));
        } finally {
            nodesLock.writeLock().unlock();
        }
    }

    @Override
    public void removeNode(int id) throws RemoteException {
        nodesLock.writeLock().lock();
        try {
            logger.debug("Remove id=" + id);
            node.removeNode(id);
            logger.debug("Current nodes=" + Arrays.toString(node.getNodes().entrySet().toArray()));
        } finally {
            nodesLock.writeLock().unlock();
        }
    }

    @Override
    public void updateItems(@NotNull List<Item> items) throws RemoteException {
        itemsLock.writeLock().lock();
        try {
            logger.debug("Update items=" + Arrays.toString(items.toArray()));
            node.putItems(items);
            StorageUtil.write(node);
            logger.debug("Current items=" + Arrays.toString(node.getItems().keySet().toArray()));
        } finally {
            itemsLock.writeLock().unlock();
        }
    }

    @Override
    public void removeItems(@NotNull List<Item> items) throws RemoteException {
        itemsLock.writeLock().lock();
        try {
            logger.debug("Remove items=" + Arrays.toString(items.toArray()));
            node.removeItems(items);
            StorageUtil.write(node);
            logger.debug("Current items=" + Arrays.toString(node.getItems().keySet().toArray()));
        } finally {
            itemsLock.writeLock().unlock();
        }
    }

    @Override
    public void updateReplicas(@NotNull List<Item> replicas) throws RemoteException {
        replicasLock.writeLock().lock();
        try {
            logger.debug("Update replicas=" + Arrays.toString(replicas.toArray()));
            node.putReplicas(replicas);
            StorageUtil.write(node);
            logger.debug("Current replicas=" + Arrays.toString(node.getReplicas().keySet().toArray()));
        } finally {
            replicasLock.writeLock().unlock();
        }
    }

    @Override
    public void removeReplicas(@NotNull List<Item> replicas) throws RemoteException {
        replicasLock.writeLock().lock();
        try {
            logger.debug("Remove replicas=" + Arrays.toString(replicas.toArray()));
            node.removeReplicas(replicas);
            StorageUtil.write(node);
            logger.debug("Current replicas=" + Arrays.toString(node.getReplicas().keySet().toArray()));
        } finally {
            replicasLock.writeLock().unlock();
        }
    }

    @Nullable
    @Override
    public Item getItem(int key) throws RemoteException {
        clientLock.readLock().lock();
        try {
            logger.debug("Get replica item with key=" + key);
            Item item = getLatestVersion(getReplicas(key));
            logger.debug("Got replica item=" + item);
            return item;
        } finally {
            clientLock.readLock().unlock();
        }
    }

    @Nullable
    @Override
    public Item updateItem(int key, @NotNull String value) throws RemoteException {
        clientLock.writeLock().lock();
        try {
            logger.debug("Update replica item with key=" + key + ", value=" + value);
            Item item = updateReplicas(key, value);
            logger.debug("Updated replica item=" + item);
            return item;
        } finally {
            clientLock.writeLock().unlock();
        }
    }

    /**
     * Returns collection of items and replicas
     * <p/>
     * Replicas are requested concurrently and returned as soon as R replicas replied
     *
     * @param itemKey of the item
     * @return collection of items with the same item key
     * @see it.unitn.ds.Replication
     * @see it.unitn.ds.ServiceConfiguration
     */
    @NotNull
    private List<Item> getReplicas(int itemKey) throws RemoteException {
        Node nodeForItem = RemoteUtil.getNodeForItem(itemKey, node.getNodes());
        Item item = nodeForItem.getItems().get(itemKey);
        List<Item> replicas = MultithreadingUtil.getReplicas(itemKey, nodeForItem, item != null, node.getNodes());
        if (item != null) {
            logger.debug("Got original item=" + item + " from nodeForItem=" + nodeForItem);
            replicas.add(item);
        }
        return replicas;
    }

    /**
     * Creates new item if exists or updates existing item with new value and increased version number
     * <p/>
     * Replicas are updated concurrently
     * <p/>
     * Amount of replicas operational must satisfy formula [ Q == max( R , W ) ], where:
     * - Q is the number of replicas and items gotten from operational nodes
     * - R and W are read and write quorums respectively
     *
     * @param itemKey   of the item
     * @param itemValue new value of the item
     * @return created or updated item or null if not agreed on WRITE quorum [ Q != max( R , W ) ]
     * @see it.unitn.ds.Replication
     * @see it.unitn.ds.ServiceConfiguration
     */
    @Nullable
    private Item updateReplicas(int itemKey, @NotNull String itemValue) throws RemoteException {
        List<Item> replicas = getReplicas(itemKey);
        if (!replicas.isEmpty() && replicas.size() < Math.max(Replication.R, Replication.W)) {
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

    /**
     * Returns new item if exists or updates existing item with new value and increased version number
     *
     * @param itemKey   of the item
     * @param itemValue new value of the item
     * @param replicas  collection of items with the same item key
     * @return created or updated item
     */
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

    /**
     * Returns latest version of the item among all in the collection
     *
     * @param replicas collection of items with the same item key
     * @return latest version item, or null if collection is empty
     */
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
