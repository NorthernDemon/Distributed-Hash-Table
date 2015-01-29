package it.unitn.ds.server;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.List;
import java.util.TreeSet;

public interface NodeUtil extends Remote {

    Node getNode() throws RemoteException;

    TreeSet<Integer> getNodes() throws RemoteException;

    void addNode(int nodeId) throws RemoteException;

    void removeNode(int nodeId) throws RemoteException;

    void updateItems(List<Item> removedItems) throws RemoteException;
}
