package it.unitn.ds;

import it.unitn.ds.server.Item;
import it.unitn.ds.server.Node;
import it.unitn.ds.server.NodeUtil;
import it.unitn.ds.server.NodeUtilImpl;
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

    private static final String RMI_NODE = "rmi://localhost/NodeUtil";

    /**
     * ./server.jar {RMI port},{Own Node ID},[{Existing Node ID}||0, if there are no nodes yet]
     * <p/>
     * Example: [1099,10,0]
     * Example: [1100,15,10]
     *
     * @param args
     */
    public static void main(String[] args) {
        int ownNodeId = 0;
        try {
            logger.info("Server Node is ready for request>>");
            logger.info("Example: [{RMI port},{Own Node ID},{Existing Node ID}||0]");
            logger.info("Example: [1099,10,0]");
            logger.info("Example: [1100,15,10]");
            Scanner scanner = new Scanner(System.in);
            String[] commands = scanner.nextLine().split(",");
            int port = Integer.parseInt(commands[0]);
            ownNodeId = Integer.parseInt(commands[1]);
            int existingNodeId = Integer.parseInt(commands[2]);
            Node ownNode;
            if (existingNodeId == 0) {
                logger.info("NodeId=" + ownNodeId + " is the first node in circle");
                ownNode = register(ownNodeId, port);
                logger.info("NodeId=" + ownNodeId + " is connected as first node=" + ownNode);
            } else {
                logger.info("NodeId=" + ownNodeId + " connects to existing nodeId=" + existingNodeId);
                Node successorNode = getSuccessorNode(ownNodeId, ((NodeUtil) Naming.lookup(RMI_NODE + existingNodeId)).getNodes());
                ownNode = register(ownNodeId, port);
                announceJoin(ownNode, successorNode.getNodes());
                transferItems(ownNode, successorNode);
                logger.info("NodeId=" + ownNodeId + " connected as node" + ownNode);
            }
            scanner.nextLine(); // waiting for leave signal
            logger.info("NodeId=" + ownNodeId + " is disconnecting from the circle...");
            leave(ownNode);
            logger.info("NodeId=" + ownNodeId + " disconnected as node" + ownNode);
            System.exit(0);
        } catch (Exception e) {
            logger.error("RMI error", e);
            unbindRMI(ownNodeId);
        }
    }

    @Nullable
    private static Node getSuccessorNode(int ownNodeId, TreeSet<Integer> nodes) throws Exception {
        for (int nodeId : nodes) {
            if (nodeId > ownNodeId) {
                return getNode(ownNodeId, nodeId);
            }
        }
        int nodeId = nodes.iterator().next();
        if (nodeId == ownNodeId) {
            logger.warn("NodeId=" + ownNodeId + " did not find successorNode, except itself");
            return null;
        }
        return getNode(ownNodeId, nodeId);
    }

    private static Node getNode(int ownNodeId, int nodeId) throws Exception {
        logger.debug("NodeId=" + ownNodeId + " found successorNodeId=" + nodeId);
        return ((NodeUtil) Naming.lookup(RMI_NODE + nodeId)).getNode();
    }

    private static Node register(final int ownNodeId, int port) throws Exception {
        logger.debug("RMI: registering with port=" + port);
        LocateRegistry.createRegistry(port);
        Node node = new Node(ownNodeId);
        Naming.bind(RMI_NODE + ownNodeId, new NodeUtilImpl(node));
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                logger.info("Stopping the RMI...");
                unbindRMI(ownNodeId);
            }
        });
        node.getNodes().add(node.getId());
        logger.debug("RMI: Node registered=" + node);
        return node;
    }

    private static void announceJoin(Node ownNode, TreeSet<Integer> nodes) throws Exception {
        logger.debug("NodeId=" + ownNode.getId() + " announcing join to node.size()=" + nodes.size());
        for (int nodeId : nodes) {
            if (nodeId != ownNode.getId()) {
                ownNode.getNodes().add(nodeId);
                ((NodeUtil) Naming.lookup(RMI_NODE + nodeId)).addNode(ownNode.getId());
                logger.debug("NodeId=" + ownNode.getId() + " announced join to nodeId=" + nodeId);
            }
        }
    }

    private static void announceLeave(Node ownNode, TreeSet<Integer> nodes) throws Exception {
        logger.debug("NodeId=" + ownNode.getId() + " announcing leave to node.size()=" + nodes.size());
        for (int nodeId : nodes) {
            if (nodeId != ownNode.getId()) {
                ((NodeUtil) Naming.lookup(RMI_NODE + nodeId)).removeNode(ownNode.getId());
                logger.debug("NodeId=" + ownNode.getId() + " announced leave to nodeId=" + nodeId);
            }
        }
    }

    private static void transferItems(Node toNode, Node fromNode) throws Exception {
        logger.debug("Transferring items fromNode=" + fromNode.getId() + " toNode=" + toNode.getId());
        List<Item> removedItems = new ArrayList<>();
        for (Item item : fromNode.getItems().values()) {
            if (item.getKey() < toNode.getId()) {
                StorageUtil.write(toNode, item);
                removedItems.add(item);
            }
        }
        ((NodeUtil) Naming.lookup(RMI_NODE + fromNode.getId())).updateItems(removedItems);
        logger.debug("Transferred items fromNode=" + fromNode.getId() + " toNode=" + toNode.getId());
    }

    private static void leave(Node ownNode) throws Exception {
        Node successorNode = getSuccessorNode(ownNode.getId(), ownNode.getNodes());
        if (successorNode != null) {
            transferItems(successorNode, ownNode);
            announceLeave(ownNode, ownNode.getNodes());
        }
    }

    private static void unbindRMI(int ownNodeId) {
        try {
            Naming.unbind(RMI_NODE + ownNodeId);
        } catch (Exception e) {
            logger.error("Naming.unbind error", e);
        }
    }
}
