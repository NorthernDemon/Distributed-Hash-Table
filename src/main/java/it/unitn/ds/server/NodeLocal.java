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

    public void joinFirst(int port, int nodeId) throws Exception {
        logger.info("NodeId=" + nodeId + " is the first node in circle");
        register(nodeId, port);
        logger.info("NodeId=" + nodeId + " is connected as first node=" + node);
    }

    public void join(int port, int nodeId, int existingNodeId) throws Exception {
        logger.info("NodeId=" + nodeId + " connects to existing nodeId=" + existingNodeId);
        TreeSet<Integer> nodes = getRemoteNode(existingNodeId).getNodes();
        Node successorNode = getSuccessorNode(nodeId, nodes);
        register(nodeId, port);
        if (successorNode != null) {
            announceJoin(successorNode.getNodes());
            transferItems(successorNode, node);
        }
        logger.info("NodeId=" + nodeId + " connected as node=" + node + " with successorNode=" + successorNode);
    }

    public void leave() throws Exception {
        if (node != null) {
            logger.info("NodeId=" + node.getId() + " is disconnecting from the circle...");
            Node successorNode = getSuccessorNode(node.getId(), node.getNodes());
            if (successorNode != null) {
                transferItems(node, successorNode);
                announceLeave(node.getNodes());
            }
            Naming.unbind(RMI_NODE + node.getId());
            UnicastRemoteObject.unexportObject(registry, true);
            logger.info("NodeId=" + node.getId() + " disconnected.");
            node = null;
        } else {
            logger.info("Node already left.");
        }
    }

    @Nullable
    private Node getSuccessorNode(int nodeId, TreeSet<Integer> nodes) throws Exception {
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

    private Node register(final int nodeId, int port) throws Exception {
        logger.debug("RMI: registering with port=" + port);
        registry = LocateRegistry.createRegistry(port);
        node = new Node(nodeId);
        node.getNodes().add(node.getId());
        Naming.bind(RMI_NODE + nodeId, new NodeRemoteImpl(node));
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                logger.info("Auto-leaving nodeId=" + nodeId);
                try {
                    leave();
                } catch (Exception e) {
                    logger.error("Failed to leave nodeId=" + node.getId(), e);
                }
            }
        });
        logger.debug("RMI: Node registered=" + node);
        return node;
    }

    private void announceJoin(TreeSet<Integer> nodes) throws Exception {
        logger.debug("NodeId=" + node.getId() + " announcing join to node.size()=" + nodes.size());
        for (int nodeId : nodes) {
            if (nodeId != node.getId()) {
                node.getNodes().add(nodeId);
                getRemoteNode(nodeId).addNode(node.getId());
                logger.debug("NodeId=" + node.getId() + " announced join to nodeId=" + nodeId);
            }
        }
    }

    private void announceLeave(TreeSet<Integer> nodes) throws Exception {
        logger.debug("NodeId=" + node.getId() + " announcing leave to node.size()=" + nodes.size());
        for (int nodeId : nodes) {
            if (nodeId != node.getId()) {
                getRemoteNode(nodeId).removeNode(node.getId());
                logger.debug("NodeId=" + node.getId() + " announced leave to nodeId=" + nodeId);
            }
        }
    }

    private void transferItems(Node fromNode, Node toNode) throws Exception {
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

    public NodeRemote getRemoteNode(int nodeId) throws Exception {
        return ((NodeRemote) Naming.lookup(RMI_NODE + nodeId));
    }

    public boolean isConnected() {
        return node != null;
    }
}
