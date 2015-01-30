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
     * ./server.jar {RMI port},{Own Node ID},[{Existing Node ID}||0, if there are no nodes yet]
     * <p/>
     * Example: [1099,10,0]
     * Example: [1100,15,10]
     *
     * @param args
     */
    public static void main(String[] args) throws Exception {
        logger.info("Server Node is ready for request>>");
        logger.info("Example: [{methodName},{RMI port},{Own Node ID},{Existing Node ID}||0]");
        logger.info("Example: [join,1099,10,0]");
        logger.info("Example: [join,1100,15,10]");
        logger.info("Example: [leave]");
        InputUtil.readInput("it.unitn.ds.ServerLauncher");
    }

    public static void join(int port, int nodeId, int existingNodeId) {
        try {
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

    public static void leave() {
        try {
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
