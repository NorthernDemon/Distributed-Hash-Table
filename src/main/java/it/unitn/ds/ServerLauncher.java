package it.unitn.ds;

import it.unitn.ds.server.Node;
import it.unitn.ds.server.NodeLocal;
import it.unitn.ds.util.InputUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.TreeSet;

public final class ServerLauncher {

    private static final Logger logger = LogManager.getLogger();

    private static NodeLocal nodeLocal = new NodeLocal();

    /**
     * ./server.jar [{methodName},{RMI port},{Own Node ID},{Existing Node ID}||0, if this is the first node]
     * <p/>
     * Example: [join,1099,10,0]
     * Example: [join,1100,15,10]
     * Example: [leave]
     *
     * @param args
     */
    public static void main(String[] args) {
        logger.info("Server Node is ready for request>>");
        logger.info("Example: [{methodName},{RMI port},{Own Node ID},{Existing Node ID}||0, if this is the first node]");
        logger.info("Example: [join,1099,10,0]");
        logger.info("Example: [join,1100,15,10]");
        logger.info("Example: [leave]");
        InputUtil.readInput(ServerLauncher.class.getName());
    }

    /**
     * New Node will join the circle of trust based on its id and known existing node id
     * If this is the first node joining, existing id = 0
     *
     * @param port           RMI port of the new node
     * @param nodeId         id of the current node to join
     * @param existingNodeId to fetch data from, 0 if current node is first
     */
    public static void join(int port, int nodeId, int existingNodeId) {
        try {
            if (nodeLocal.getNode() != null) {
                logger.warn("Cannot join without leaving first!");
                return;
            }
            if (existingNodeId == 0) {
                logger.info("NodeId=" + nodeId + " is the first node in circle");
                nodeLocal.register(nodeId, port);
                logger.info("NodeId=" + nodeId + " is connected as first node=" + nodeLocal.getNode());
            } else {
                logger.info("NodeId=" + nodeId + " connects to existing nodeId=" + existingNodeId);
                TreeSet<Integer> nodes = nodeLocal.getRemoteNode(existingNodeId).getNodes();
                Node successorNode = nodeLocal.getSuccessorNode(nodeId, nodes);
                nodeLocal.register(nodeId, port);
                if (successorNode != null) {
                    nodeLocal.announceJoin(successorNode.getNodes());
                    nodeLocal.transferItems(successorNode, nodeLocal.getNode());
                }
                logger.info("NodeId=" + nodeId + " connected as node=" + nodeLocal.getNode() + " with successorNode=" + successorNode);
            }
        } catch (Exception e) {
            logger.error("RMI error", e);
            System.exit(1);
        }
    }

    /**
     * Current Node will leave the circle of trust
     */
    public static void leave() {
        try {
            if (nodeLocal.getNode() == null) {
                logger.warn("Cannot leave without joining first!");
                return;
            }
            int nodeId = nodeLocal.getNode().getId();
            logger.info("NodeId=" + nodeId + " is disconnecting from the circle...");
            nodeLocal.leave();
            logger.info("NodeId=" + nodeId + " disconnected as node=" + nodeLocal.getNode());
            nodeLocal.setNode(null);
        } catch (Exception e) {
            logger.error("RMI error", e);
            System.exit(1);
        }
    }
}
