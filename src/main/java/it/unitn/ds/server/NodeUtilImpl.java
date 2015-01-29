package it.unitn.ds.server;

import it.unitn.ds.util.StorageUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class NodeUtilImpl extends UnicastRemoteObject implements NodeUtil {

    private static final Logger logger = LogManager.getLogger();

    private Node node;

    public NodeUtilImpl(Node node) throws RemoteException {
        this.node = node;
    }

    @Override
    public List<Node> getNodes() throws RemoteException {
        logger.debug("Get Nodes request=" + Arrays.toString(node.getNodes().keySet().toArray()));
        return new ArrayList<>(node.getNodes().values());
    }

    @Override
    public void addNode(Node node) throws RemoteException {
        logger.debug("Add node request with node=" + node);
        this.node.getNodes().put(node.getId(), node);
    }

    @Override
    public void removeNode(Node node) throws RemoteException {
        logger.debug("Remove node request with node=" + node);
        this.node.getNodes().remove(node.getId());
    }

    @Override
    public void updateItems(List<Item> items) throws RemoteException {
        logger.debug("Update items request with items=" + Arrays.toString(items.toArray()));
        for (Item item : items) {
            StorageUtil.write(node, item);
        }
    }
}
