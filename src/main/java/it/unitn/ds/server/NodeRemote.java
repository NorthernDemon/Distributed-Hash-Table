package it.unitn.ds.server;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.List;
import java.util.Map;

public interface NodeRemote extends Remote {

    Node getNode() throws RemoteException;

    Map<Integer, String> getNodes() throws RemoteException;

    void addNode(int nodeId, String host) throws RemoteException;

    void removeNode(int nodeId) throws RemoteException;

    void updateItems(List<Item> items) throws RemoteException;

    void removeItems(List<Item> items) throws RemoteException;

    Item getItem(int key) throws RemoteException;

    Item updateItem(int key, String value) throws RemoteException;
}
