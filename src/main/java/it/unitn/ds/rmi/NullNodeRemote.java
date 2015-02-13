package it.unitn.ds.rmi;

import it.unitn.ds.entity.Item;
import it.unitn.ds.entity.Node;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.List;
import java.util.Map;

public final class NullNodeRemote extends UnicastRemoteObject implements NodeServer, NodeClient {

    private final Node node = new Node();

    private final Item item = new Item();

    public NullNodeRemote() throws RemoteException {
    }

    @Override
    public Node getNode() throws RemoteException {
        return node;
    }

    @Override
    public Map<Integer, String> getNodes() throws RemoteException {
        return node.getNodes();
    }

    @Override
    public void addNode(int id, String host) throws RemoteException {
    }

    @Override
    public void removeNode(int id) throws RemoteException {
    }

    @Override
    public void updateItems(List<Item> items) throws RemoteException {
    }

    @Override
    public void removeItems(List<Item> items) throws RemoteException {
    }

    @Override
    public void updateReplicas(List<Item> replicas) throws RemoteException {
    }

    @Override
    public void removeReplicas(List<Item> replicas) throws RemoteException {
    }

    @Override
    public Item getItem(int key) throws RemoteException {
        return item;
    }

    @Override
    public Item updateItem(int key, String value) throws RemoteException {
        return item;
    }
}
