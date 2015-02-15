package it.unitn.ds.rmi;

import it.unitn.ds.entity.Item;
import it.unitn.ds.entity.Node;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.List;
import java.util.Map;

/**
 * Interface to be used by SERVER for accessing the remote node via RMI
 */
public interface NodeServer extends Remote {

    Node getNode() throws RemoteException;

    Map<Integer, String> getNodes() throws RemoteException;

    void addNode(int id, String host) throws RemoteException;

    void removeNode(int id) throws RemoteException;

    void updateItems(List<Item> items) throws RemoteException;

    void removeItems(List<Item> items) throws RemoteException;

    void updateReplicas(List<Item> replicas) throws RemoteException;

    void removeReplicas(List<Item> replicas) throws RemoteException;
}
