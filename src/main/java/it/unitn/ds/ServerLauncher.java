package it.unitn.ds;

import it.unitn.ds.server.Item;
import it.unitn.ds.server.Node;
import it.unitn.ds.server.NodeRemote;
import it.unitn.ds.server.NodeRemoteImpl;
import it.unitn.ds.util.StorageUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.Nullable;

import java.rmi.Naming;
import java.rmi.registry.LocateRegistry;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.TreeSet;

public final class ServerLauncher {

    private static final Logger logger = LogManager.getLogger();

    private static final String RMI_NODE = "rmi://localhost/NodeRemote";

    /**
     * ./server.jar {RMI port},{Own Node ID},[{Existing Node ID}||0, if there are no nodes yet]
     * <p/>
     * Example: [1099,10,0]
     * Example: [1100,15,10]
     *
     * @param args
     */
    public static void main(String[] args) {
        try {
            logger.info("Server Node is ready for request>>");
            logger.info("Example: [{RMI port},{Own Node ID},{Existing Node ID}||0]");
            logger.info("Example: [1099,10,0]");
            logger.info("Example: [1100,15,10]");
            Scanner scanner = new Scanner(System.in);
            String[] commands = scanner.nextLine().split(",");
            int port = Integer.parseInt(commands[0]);
            int nodeId = Integer.parseInt(commands[1]);
            int existingNodeId = Integer.parseInt(commands[2]);
            Node node;
            if (existingNodeId == 0) {
                logger.info("NodeId=" + nodeId + " is the first node in circle");
                node = register(nodeId, port);
                logger.info("NodeId=" + nodeId + " is connected as first node=" + node);
            } else {
                logger.info("NodeId=" + nodeId + " connects to existing nodeId=" + existingNodeId);
                TreeSet<Integer> nodes = getRemoteNode(existingNodeId).getNodes();
                Node successorNode = getSuccessorNode(nodeId, nodes);
                node = register(nodeId, port);
                if (successorNode != null) {
                    announceJoin(node, successorNode.getNodes());
                    transferItems(successorNode, node);
                }
                logger.info("NodeId=" + nodeId + " connected as node=" + node + " with successorNode=" + successorNode);
            }
            logger.info("Press [ENTER] to leave");
            scanner.nextLine(); // waiting for leave signal
            logger.info("NodeId=" + nodeId + " is disconnecting from the circle...");
            leave(node);
            logger.info("NodeId=" + nodeId + " disconnected as node=" + node);
            System.exit(0);
        } catch (Exception e) {
            logger.error("RMI error", e);
            System.exit(1);
        }
    }

    @Nullable
    private static Node getSuccessorNode(int nodeId, TreeSet<Integer> nodes) throws Exception {
        logger.debug("NodeId=" + nodeId + " is searching for successorNode...");
        int successorNodeId = getSuccessorNodeId(nodeId, nodes);
        if (successorNodeId == nodeId) {
            logger.warn("NodeId=" + nodeId + " did not find successorNode, except itself");
            return null;
        }
        logger.debug("NodeId=" + nodeId + " found successorNodeId=" + successorNodeId);
        return getRemoteNode(successorNodeId).getNode();
    }

    private static int getSuccessorNodeId(int targetNodeId, TreeSet<Integer> nodes) {
        for (int nodeId : nodes) {
            if (nodeId > targetNodeId) {
                return nodeId;
            }
        }
        return nodes.iterator().next();
    }

    private static Node register(final int nodeId, int port) throws Exception {
        logger.debug("RMI: registering with port=" + port);
        LocateRegistry.createRegistry(port);
        Node node = new Node(nodeId);
        node.getNodes().add(node.getId());
        Naming.bind(RMI_NODE + nodeId, new NodeRemoteImpl(node));
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                logger.info("Stopping the RMI of nodeId=" + nodeId);
                try {
                    Naming.unbind(RMI_NODE + nodeId);
                } catch (Exception e) {
                    logger.error("Failed to Naming.unbind() for nodeId=" + nodeId, e);
                }
            }
        });
        logger.debug("RMI: Node registered=" + node);
        return node;
    }

    private static void announceJoin(Node node, TreeSet<Integer> nodes) throws Exception {
        logger.debug("NodeId=" + node.getId() + " announcing join to node.size()=" + nodes.size());
        for (int nodeId : nodes) {
            if (nodeId != node.getId()) {
                node.getNodes().add(nodeId);
                getRemoteNode(nodeId).addNode(node.getId());
                logger.debug("NodeId=" + node.getId() + " announced join to nodeId=" + nodeId);
            }
        }
    }

    private static void announceLeave(Node node, TreeSet<Integer> nodes) throws Exception {
        logger.debug("NodeId=" + node.getId() + " announcing leave to node.size()=" + nodes.size());
        for (int nodeId : nodes) {
            if (nodeId != node.getId()) {
                getRemoteNode(nodeId).removeNode(node.getId());
                logger.debug("NodeId=" + node.getId() + " announced leave to nodeId=" + nodeId);
            }
        }
    }

    private static void transferItems(Node fromNode, Node toNode) throws Exception {
        if (fromNode.getItems().isEmpty()) {
            logger.debug("Nothing to transfer fromNode=" + fromNode);
            return;
        }
        logger.debug("Transferring items fromNode=" + fromNode.getId() + " toNode=" + toNode.getId());
        getRemoteNode(fromNode.getId()).updateItems(getRemovedItems(toNode, fromNode));
        logger.debug("Transferred items fromNode=" + fromNode.getId() + " toNode=" + toNode.getId());
    }

    private static List<Item> getRemovedItems(Node toNode, Node fromNode) {
        List<Item> items = new ArrayList<>();
        for (Item item : fromNode.getItems().values()) {
            if (item.getKey() <= toNode.getId()) {
                StorageUtil.write(toNode, item);
                items.add(item);
            } else {
                return items;
            }
        }
        return items;
    }

    private static void leave(Node node) throws Exception {
        Node successorNode = getSuccessorNode(node.getId(), node.getNodes());
        if (successorNode != null) {
            transferItems(node, successorNode);
            announceLeave(node, node.getNodes());
        }
    }

    private static NodeRemote getRemoteNode(int nodeId) throws Exception {
        return ((NodeRemote) Naming.lookup(RMI_NODE + nodeId));
    }
}
