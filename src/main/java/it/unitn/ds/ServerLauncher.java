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
import java.util.*;

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
            Node successorNode = null;
            Node ownNode;
            if (existingNodeId == 0) {
                logger.info("NodeId=" + ownNodeId + " is the first node in circle");
                ownNode = register(ownNodeId, port);
                logger.info("NodeId=" + ownNodeId + " is connected as first node=" + ownNode);
            } else {
                logger.info("NodeId=" + ownNodeId + " connects to existing nodeId=" + existingNodeId);
                successorNode = getSuccessorNode(ownNodeId, ((NodeUtil) Naming.lookup(RMI_NODE + existingNodeId)).getNodes());
                ownNode = register(ownNodeId, port);
                announceJoin(ownNode, successorNode.getNodes().values());
                transferItems(ownNode, successorNode);
                logger.info("NodeId=" + ownNodeId + " connected as node" + ownNode);
            }
            String line = scanner.nextLine();
            if (line.equals("leave")) {
                logger.info("NodeId=" + ownNodeId + " is disconnecting from the circle...");
                if (successorNode == null) {
                    successorNode = getSuccessorNode(ownNodeId, ((NodeUtil) Naming.lookup(RMI_NODE + existingNodeId)).getNodes());
                }
                transferItems(successorNode, ownNode);
                announceLeave(ownNode, successorNode.getNodes().values());
                logger.info("NodeId=" + ownNodeId + " disconnected as node" + ownNode);
            }
        } catch (Exception e) {
            logger.error("RMI error", e);
            try {
                Naming.unbind(RMI_NODE + ownNodeId);
            } catch (Exception e1) {
                logger.error("Naming.unbind error", e);
            }
        }
    }

    @Nullable
    private static Node getSuccessorNode(int ownNodeId, List<Node> nodes) {
        for (Node node : nodes) {
            if (node.getId() > ownNodeId) {
                logger.debug("NodeId=" + ownNodeId + " found successorNode=" + node);
                return node;
            }
        }
        Iterator<Node> iterator = nodes.iterator();
        if (iterator.hasNext()) {
            Node node = iterator.next();
            logger.debug("NodeId=" + ownNodeId + " found successorNode=" + node);
            return node;
        } else {
            logger.warn("NodeId=" + ownNodeId + " did not find successorNode");
            return null;
        }
    }

    private static Node register(int ownNodeId, int port) throws Exception {
        logger.debug("RMI: registering with port=" + port);
        LocateRegistry.createRegistry(port);
        Node node = new Node(ownNodeId);
        Naming.bind(RMI_NODE + ownNodeId, new NodeUtilImpl(node));
        node.getNodes().put(node.getId(), node);
        logger.debug("RMI: Node registered=" + node);
        return node;
    }

    private static void announceJoin(Node ownNode, Collection<Node> nodes) throws Exception {
        logger.debug("NodeId=" + ownNode.getId() + " announcing join to node.size()=" + nodes.size());
        for (Node node : nodes) {
            ownNode.getNodes().put(node.getId(), node);
            ((NodeUtil) Naming.lookup(RMI_NODE + node.getId())).addNode(ownNode);
            logger.debug("NodeId=" + ownNode.getId() + " announced join to node=" + node);
        }
    }

    private static void announceLeave(Node ownNode, Collection<Node> nodes) throws Exception {
        logger.debug("NodeId=" + ownNode.getId() + " announcing leave to node.size()=" + nodes.size());
        ownNode.getNodes().remove(ownNode.getId());
        for (Node node : nodes) {
            ownNode.getNodes().remove(node.getId());
            ((NodeUtil) Naming.lookup(RMI_NODE + node.getId())).removeNode(ownNode);
            logger.debug("NodeId=" + ownNode.getId() + " announced leave to node=" + node);
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
}
