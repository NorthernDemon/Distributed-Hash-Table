package it.unitn.ds;

import it.unitn.ds.server.Item;
import it.unitn.ds.server.Node;
import it.unitn.ds.server.NodeUtil;
import it.unitn.ds.server.NodeUtilImpl;
import it.unitn.ds.util.StorageUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.rmi.Naming;
import java.rmi.registry.LocateRegistry;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Scanner;

public final class ServerLauncher {

    private static final Logger logger = LogManager.getLogger();

    private static final int RMI_PORT = 1099;

    private static final String RMI_NODE = "rmi://localhost/NodeUtil";

    /**
     * ./server.jar {Own Node ID},[{Existing Node ID}||0, if there are no nodes yet]
     * <p/>
     * Example: [10,0]
     * Example: [15,10]
     *
     * @param args
     */
    public static void main(String[] args) {
        try {
            logger.info("Server Node is ready for request>>");
            logger.info("Example: [{Own Node ID,{Existing Node ID}]");
            logger.info("Example: [10,0]");
            logger.info("Example: [15,10]");
            Scanner scanner = new Scanner(System.in);
            String[] commands = scanner.nextLine().split(",");
            int ownNodeId = Integer.parseInt(commands[0]);
            int existingNodeId = Integer.parseInt(commands[1]);
            Node successorNode = null;
            Node ownNode;
            if (existingNodeId == 0) {
                logger.info("NodeId=" + ownNodeId + " is the first node in circle");
                ownNode = register(ownNodeId);
            } else {
                logger.info("NodeId=" + ownNodeId + " connects to existing nodeId=" + existingNodeId);
                successorNode = getSuccessorNode(ownNodeId, ((NodeUtil) Naming.lookup(RMI_NODE + existingNodeId)).getNodes());
                ownNode = register(ownNodeId);
                announceJoined(ownNode, successorNode.getNodes().values());
                transferItems(ownNode, successorNode);
            }
            String line = scanner.nextLine();
            if (line.equals("leave")) {
                logger.info("NodeId=" + ownNodeId + " is leaving the circle...");
                if (successorNode == null) {
                    successorNode = getSuccessorNode(ownNodeId, ((NodeUtil) Naming.lookup(RMI_NODE + existingNodeId)).getNodes());
                }
                transferItems(successorNode, ownNode);
                announceLeft(ownNode, successorNode.getNodes().values());
            }
        } catch (Exception e) {
            logger.error("RMI error", e);
        }
    }

    private static Node getSuccessorNode(int ownNodeId, List<Node> nodes) {
        for (Node node : nodes) {
            if (node.getId() > ownNodeId) {
                return node;
            }
        }
        return nodes.iterator().next();
    }

    private static Node register(int ownNodeId) throws Exception {
        logger.info("RMI: registering with default port=" + RMI_PORT);
        LocateRegistry.createRegistry(RMI_PORT);
        Node node = new Node(ownNodeId);
        Naming.bind(RMI_NODE + ownNodeId, new NodeUtilImpl(node));
        logger.info("RMI: Node registered=" + node);
        return node;
    }

    private static void announceJoined(Node ownNode, Collection<Node> nodes) throws Exception {
        for (Node node : nodes) {
            ((NodeUtil) Naming.lookup(RMI_NODE + node.getId())).addNode(ownNode);
        }
    }

    private static void announceLeft(Node ownNode, Collection<Node> nodes) throws Exception {
        for (Node node : nodes) {
            ((NodeUtil) Naming.lookup(RMI_NODE + node.getId())).removeNode(ownNode);
        }
    }

    private static void transferItems(Node toNode, Node fromNode) throws Exception {
        List<Item> removedItems = new ArrayList<>();
        for (Item item : fromNode.getItems().values()) {
            if (item.getKey() < toNode.getId()) {
                StorageUtil.write(toNode, item);
                removedItems.add(item);
            }
        }
        ((NodeUtil) Naming.lookup(RMI_NODE + fromNode.getId())).updateItems(removedItems);
    }
}
