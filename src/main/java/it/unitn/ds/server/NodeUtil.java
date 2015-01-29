package it.unitn.ds.server;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.List;

public interface NodeUtil extends Remote {

    List<Node> getNodes() throws RemoteException;

    void addNode(Node ownNode) throws RemoteException;

    void updateItems(List<Item> removedItems) throws RemoteException;
}
