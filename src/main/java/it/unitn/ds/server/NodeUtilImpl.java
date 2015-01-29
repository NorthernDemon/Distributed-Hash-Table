package it.unitn.ds.server;

import it.unitn.ds.util.StorageUtil;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class NodeUtilImpl extends UnicastRemoteObject implements NodeUtil {

    private Node node;

    public NodeUtilImpl(Node node) throws RemoteException {
        this.node = node;
    }

    @Override
    public List<Node> getNodes() throws RemoteException {
        return new ArrayList<>(node.getNodes().values());
    }

    @Override
    public void addNode(Node node) throws RemoteException {
        this.node.getNodes().put(node.getId(), node);
    }

    @Override
    public void removeNode(Node node) throws RemoteException {
        this.node.getNodes().remove(node.getId());
    }

    @Override
    public void updateItems(List<Item> items) throws RemoteException {
        for (Item item : items) {
            StorageUtil.write(node, item);
        }
    }
}
