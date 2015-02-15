package it.unitn.ds.rmi;

import it.unitn.ds.entity.Item;
import it.unitn.ds.entity.Node;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Used to simulate crashed node or in case of network errors
 */
public final class NullNodeRemote extends UnicastRemoteObject implements NodeServer, NodeClient {

    @NotNull
    private final Node node;

    public NullNodeRemote(@NotNull Node node) throws RemoteException {
        this.node = node;
    }

    @NotNull
    @Override
    public Node getNode() throws RemoteException {
        return node;
    }

    @NotNull
    @Override
    public Map<Integer, String> getNodes() throws RemoteException {
        return Collections.emptyMap();
    }

    @Override
    public void addNode(int id, @NotNull String host) throws RemoteException {
    }

    @Override
    public void removeNode(int id) throws RemoteException {
    }

    @Override
    public void updateItems(@NotNull List<Item> items) throws RemoteException {
    }

    @Override
    public void removeItems(@NotNull List<Item> items) throws RemoteException {
    }

    @Override
    public void updateReplicas(@NotNull List<Item> replicas) throws RemoteException {
    }

    @Override
    public void removeReplicas(@NotNull List<Item> replicas) throws RemoteException {
    }

    @Nullable
    @Override
    public Item getItem(int key) throws RemoteException {
        return null;
    }

    @Nullable
    @Override
    public Item updateItem(int key, @NotNull String value) throws RemoteException {
        return null;
    }
}
