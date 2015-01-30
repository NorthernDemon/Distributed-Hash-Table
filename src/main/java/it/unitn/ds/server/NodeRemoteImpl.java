package it.unitn.ds.server;

import it.unitn.ds.util.StorageUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.Arrays;
import java.util.List;
import java.util.TreeSet;

public class NodeRemoteImpl extends UnicastRemoteObject implements NodeRemote {

    private static final Logger logger = LogManager.getLogger();

    private Node node;

    public NodeRemoteImpl(Node node) throws RemoteException {
        this.node = node;
    }

    @Override
    public Node getNode() throws RemoteException {
        logger.debug("Get node=" + node);
        return node;
    }

    @Override
    public TreeSet<Integer> getNodes() throws RemoteException {
        logger.debug("Get nodes=" + Arrays.toString(node.getNodes().toArray()));
        return node.getNodes();
    }

    @Override
    public void addNode(int nodeId) throws RemoteException {
        logger.debug("Add node request with node=" + nodeId);
        node.getNodes().add(nodeId);
        logger.debug("Current nodes=" + Arrays.toString(node.getNodes().toArray()));
    }

    @Override
    public void removeNode(int nodeId) throws RemoteException {
        logger.debug("Remove node request with node=" + nodeId);
        node.getNodes().remove(nodeId);
        logger.debug("Current nodes=" + Arrays.toString(node.getNodes().toArray()));
    }

    @Override
    public void updateItems(List<Item> items) throws RemoteException {
        logger.debug("Update items request with items=" + Arrays.toString(items.toArray()));
        StorageUtil.write(node, items);
        logger.debug("Current items=" + Arrays.toString(node.getItems().values().toArray()));
    }
}
