package it.unitn.ds.server;

import it.unitn.ds.util.RemoteUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.Nullable;

import java.rmi.Naming;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.TreeSet;

public final class NodeLocal {

    private static final Logger logger = LogManager.getLogger();

    @Nullable
    private Node node;

    private Registry registry;

    public void joinFirst(int port, int nodeId) throws Exception {
        logger.info("NodeId=" + nodeId + " is the first node in circle");
        register(nodeId, port);
        logger.info("NodeId=" + nodeId + " is connected as first node=" + node);
    }

    public void join(int port, int nodeId, int existingNodeId) throws Exception {
        logger.info("NodeId=" + nodeId + " connects to existing nodeId=" + existingNodeId);
        TreeSet<Integer> nodes = RemoteUtil.getRemoteNode(existingNodeId).getNodes();
        Node successorNode = RemoteUtil.getSuccessorNode(nodeId, nodes);
        register(nodeId, port);
        if (successorNode != null) {
            RemoteUtil.announceJoin(node, successorNode.getNodes());
            RemoteUtil.transferItems(successorNode, node);
        }
        logger.info("NodeId=" + nodeId + " connected as node=" + node + " with successorNode=" + successorNode);
    }

    public void leave() throws Exception {
        if (node != null) {
            logger.info("NodeId=" + node.getId() + " is disconnecting from the circle...");
            Node successorNode = RemoteUtil.getSuccessorNode(node.getId(), node.getNodes());
            if (successorNode != null) {
                RemoteUtil.transferItems(node, successorNode);
                RemoteUtil.announceLeave(node, node.getNodes());
            }
            Naming.unbind(RemoteUtil.RMI_NODE + node.getId());
            UnicastRemoteObject.unexportObject(registry, true);
            logger.info("NodeId=" + node.getId() + " disconnected.");
            node = null;
        } else {
            logger.info("Node already left.");
        }
    }

    private Node register(final int nodeId, int port) throws Exception {
        logger.debug("RMI: registering with port=" + port);
        registry = LocateRegistry.createRegistry(port);
        node = new Node(nodeId);
        node.getNodes().add(node.getId());
        Naming.bind(RemoteUtil.RMI_NODE + nodeId, new NodeRemoteImpl(node));
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                logger.info("Auto-leaving nodeId=" + nodeId);
                try {
                    leave();
                } catch (Exception e) {
                    logger.error("Failed to leave node", e);
                }
            }
        });
        logger.debug("RMI: Node registered=" + node);
        return node;
    }

    public boolean isConnected() {
        return node != null;
    }
}
