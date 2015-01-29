package it.unitn.ds.server;

import it.unitn.ds.Item;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.List;

public class NodeUtilImpl extends UnicastRemoteObject implements NodeUtil {

    private Node node;

    public NodeUtilImpl(Node node) throws RemoteException {
        this.node = node;
    }

    @Override
    public List<Node> getNodes() throws RemoteException {
        return node.getNodes();
    }

    @Override
    public void addNode(Node node) throws RemoteException {
        this.node.getNodes().add(node);
    }

    @Override
    public void removeItems(List<Item> removedItems) throws RemoteException {
        this.node.getItems().retainAll(removedItems);
    }
}
