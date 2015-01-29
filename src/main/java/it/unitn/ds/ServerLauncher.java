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
import java.util.*;

public final class ServerLauncher {

    private static final Logger logger = LogManager.getLogger();

    /**
     * ./server.jar {Own Node ID},{Existing Node ID}
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
            String[] commands = new Scanner(System.in).nextLine().split(",");
            int ownNodeId = Integer.parseInt(commands[0]);
            int existingNodeId = Integer.parseInt(commands[1]);
            if (existingNodeId == 0) {
                logger.info("NodeId=" + ownNodeId + " is the first node in circle");
                register(ownNodeId);
            } else {
                logger.info("NodeId=" + ownNodeId + " connects to existing nodeId=" + existingNodeId);
                NodeUtil existingNode = (NodeUtil) Naming.lookup("rmi://localhost/NodeUtil" + existingNodeId);
                Node successorNode = getSuccessorNode(ownNodeId, existingNode.getNodes());
                Node ownNode = register(ownNodeId);
                announce(ownNode, successorNode.getNodes());
                transferItems(ownNode, successorNode);
            }
        } catch (Exception e) {
            logger.error(e);
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

    private static Node register(int ownNodeId) throws Exception {
        int port = 1100 + new Random().nextInt(10);
        LocateRegistry.createRegistry(1099);
        logger.info("RMI is up at port=" + port);
        Node node = new Node(ownNodeId);
        Naming.bind("rmi:///NodeUtil" + ownNodeId, new NodeUtilImpl(node));
        logger.info("Node registered=" + node);
        return node;
    }

    private static void announce(Node ownNode, Map<Integer, Node> nodes) throws Exception {
        for (Node node : nodes.values()) {
            NodeUtil remoteNode = (NodeUtil) Naming.lookup("rmi://localhost/NodeUtil" + node.getId());
            remoteNode.addNode(ownNode);
        }
    }

    private static void transferItems(Node ownNode, Node successorNode) throws Exception {
        List<Item> removedItems = new ArrayList<>();
        for (Item item : successorNode.getItems().values()) {
            if (item.getKey() < ownNode.getId()) {
                StorageUtil.write(ownNode, item);
                removedItems.add(item);
            }
        }
        NodeUtil remoteSuccessor = (NodeUtil) Naming.lookup("rmi://localhost/NodeUtil" + successorNode.getId());
        remoteSuccessor.updateItems(removedItems);
    }
}
