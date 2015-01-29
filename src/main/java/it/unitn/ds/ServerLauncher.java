package it.unitn.ds;

import it.unitn.ds.server.Node;
import it.unitn.ds.server.NodeUtil;
import it.unitn.ds.server.NodeUtilImpl;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.rmi.Naming;
import java.rmi.registry.LocateRegistry;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public final class ServerLauncher {

    private static final Logger logger = LogManager.getLogger();

    /**
     * ./server.jar {Own Node ID} {Existing Node ID}
     *
     * @param args
     */
    public static void main(String[] args) {
        int ownNodeId = Integer.parseInt(args[0]);
        int existingNodeId = Integer.parseInt(args[1]);
        logger.info("NodeId=" + ownNodeId + " connects to existing nodeId=" + existingNodeId);
        try {
            // Request existing node
            NodeUtil existingNode = (NodeUtil) Naming.lookup("rmi://localhost/NodeUtil" + existingNodeId);
            // FIXME nodes must be sorted from 0 to max
            Node successorNode = getSuccessorNode(ownNodeId, existingNode.getNodes());
            Node ownNode = register(ownNodeId);
            announce(ownNode, successorNode);
            transferItems(ownNode, successorNode);
        } catch (Exception e) {
            logger.error(e);
        }
    }

    private static void transferItems(Node ownNode, Node successorNode) throws Exception {
        List<Item> removedItems = new ArrayList<>();
        for (Item item : successorNode.getItems().values()) {
            if (item.getKey() < ownNode.getId()) {
                ownNode.getItems().put(item.getKey(), item);
                removedItems.add(item);
            }
        }
        NodeUtil remoteSuccessor = (NodeUtil) Naming.lookup("rmi://localhost/NodeUtil" + successorNode.getId());
        remoteSuccessor.removeItems(removedItems);
    }

    private static Node register(int ownNodeId) throws Exception {
        int port = 1100 + new Random().nextInt(1000);
        LocateRegistry.createRegistry(port);
        logger.info("RMI is up at port=" + port);
        Node node = new Node(ownNodeId);
        Naming.bind("rmi:///NodeUtil" + ownNodeId, new NodeUtilImpl(node));
        logger.info("Node registered=" + node);
        return node;
    }

    private static void announce(Node ownNode, Node successorNode) throws Exception {
        for (Node node : successorNode.getNodes().values()) {
            NodeUtil remoteNode = (NodeUtil) Naming.lookup("rmi://localhost/NodeUtil" + node.getId());
            remoteNode.addNode(ownNode);
        }
    }

    private static Node getSuccessorNode(int ownNodeId, List<Node> nodes) {
        for (Node node : nodes) {
            if (node.getId() > ownNodeId) {
                return node;
            }
        }
        return nodes.get(0);
    }
}
