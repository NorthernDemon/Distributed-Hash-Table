package it.unitn.ds.server;

import it.unitn.ds.util.StorageUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.Nullable;

import java.rmi.Naming;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;

public class NodeLocal {

    private static final Logger logger = LogManager.getLogger();

    private static final String RMI_NODE = "rmi://localhost/NodeRemote";

    private Node node;

    private Registry registry;

    @Nullable
    public Node getSuccessorNode(int nodeId, TreeSet<Integer> nodes) throws Exception {
        logger.debug("NodeId=" + nodeId + " is searching for successorNode...");
        int successorNodeId = getSuccessorNodeId(nodeId, nodes);
        if (successorNodeId == nodeId) {
            logger.debug("NodeId=" + nodeId + " did not find successorNode, except itself");
            return null;
        }
        logger.debug("NodeId=" + nodeId + " found successorNodeId=" + successorNodeId);
        return getRemoteNode(successorNodeId).getNode();
    }

    private int getSuccessorNodeId(int targetNodeId, TreeSet<Integer> nodes) {
        for (int nodeId : nodes) {
            if (nodeId > targetNodeId) {
                return nodeId;
            }
        }
        return nodes.iterator().next();
    }

    public Node register(final int nodeId, int port) throws Exception {
        logger.debug("RMI: registering with port=" + port);
        registry = LocateRegistry.createRegistry(port);
        node = new Node(nodeId);
        node.getNodes().add(node.getId());
        Naming.bind(RMI_NODE + nodeId, new NodeRemoteImpl(node));
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                logger.info("Stopping the RMI of nodeId=" + nodeId);
                closeRMI();
            }
        });
        logger.debug("RMI: Node registered=" + node);
        return node;
    }

    public void announceJoin(TreeSet<Integer> nodes) throws Exception {
        logger.debug("NodeId=" + node.getId() + " announcing join to node.size()=" + nodes.size());
        for (int nodeId : nodes) {
            if (nodeId != node.getId()) {
                node.getNodes().add(nodeId);
                getRemoteNode(nodeId).addNode(node.getId());
                logger.debug("NodeId=" + node.getId() + " announced join to nodeId=" + nodeId);
            }
        }
    }

    public void announceLeave(TreeSet<Integer> nodes) throws Exception {
        logger.debug("NodeId=" + node.getId() + " announcing leave to node.size()=" + nodes.size());
        for (int nodeId : nodes) {
            if (nodeId != node.getId()) {
                getRemoteNode(nodeId).removeNode(node.getId());
                logger.debug("NodeId=" + node.getId() + " announced leave to nodeId=" + nodeId);
            }
        }
    }

    public void transferItems(Node fromNode, Node toNode) throws Exception {
        if (fromNode.getItems().isEmpty()) {
            logger.debug("Nothing to transfer fromNode=" + fromNode + " toNode=" + toNode);
            return;
        }
        logger.debug("Transferring items fromNode=" + fromNode + " toNode=" + toNode);
        getRemoteNode(fromNode.getId()).updateItems(getRemovedItems(toNode, fromNode));
        logger.debug("Transferred items fromNode=" + fromNode + " toNode=" + toNode);
    }

    private List<Item> getRemovedItems(Node toNode, Node fromNode) {
        List<Item> items = new ArrayList<>();
        for (Item item : fromNode.getItems().values()) {
            if (item.getKey() <= toNode.getId()) {
                items.add(item);
            } else {
                break;
            }
        }
        StorageUtil.write(toNode, items);
        return items;
    }

    public void leave() throws Exception {
        Node successorNode = getSuccessorNode(node.getId(), node.getNodes());
        if (successorNode != null) {
            transferItems(node, successorNode);
            announceLeave(node.getNodes());
        }
        closeRMI();
    }

    private void closeRMI() {
        if (node != null) {
            try {
                Naming.unbind(RMI_NODE + node.getId());
                UnicastRemoteObject.unexportObject(registry, true);
            } catch (Exception e) {
                logger.error("Failed to Naming.unbind() for nodeId=" + node.getId(), e);
            }
            node = null;
        }
    }

    public NodeRemote getRemoteNode(int nodeId) throws Exception {
        return ((NodeRemote) Naming.lookup(RMI_NODE + nodeId));
    }

    @Nullable
    public Node getNode() {
        return node;
    }
}
