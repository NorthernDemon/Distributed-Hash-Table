package it.unitn.ds.rmi;

import it.unitn.ds.entity.Item;
import it.unitn.ds.entity.Node;
import org.jetbrains.annotations.NotNull;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.List;
import java.util.Map;

/**
 * Interface to be used by SERVER for accessing the remote node via RMI
 */
public interface NodeServer extends Remote {

    @NotNull
    Node getNode() throws RemoteException;

    @NotNull
    Map<Integer, String> getNodes() throws RemoteException;

    void addNode(int id, @NotNull String host) throws RemoteException;

    void removeNode(int id) throws RemoteException;

    void updateItems(@NotNull List<Item> items) throws RemoteException;

    void removeItems(@NotNull List<Item> items) throws RemoteException;

    void updateReplicas(@NotNull List<Item> replicas) throws RemoteException;

    void removeReplicas(@NotNull List<Item> replicas) throws RemoteException;
}
