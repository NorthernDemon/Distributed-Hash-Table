package it.unitn.ds;

import it.unitn.ds.server.NodeLocal;
import it.unitn.ds.util.InputUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public final class ServerLauncher {

    private static final Logger logger = LogManager.getLogger();

    private static NodeLocal nodeLocal = new NodeLocal();

    /**
     * ./server.jar {methodName},{host},{RMI port},{Own Node ID},{Existing Node ID or 0, if this is the first node}
     * <p/>
     * Example: join,localhost,1099,10,none,0
     * Example: join,localhost,1100,15,localhost,10
     * Example: leave
     *
     * @param args
     */
    public static void main(String[] args) {
        logger.info("Server Node is ready for request>>");
        logger.info("Example: {methodName},{host},{RMI port},{Own Node ID},{Existing Node ID or 0, if this is the first node}");
        logger.info("Example: join,localhost,1099,10,none,0");
        logger.info("Example: join,localhost,1100,15,localhost,10");
        logger.info("Example: leave");
        InputUtil.readInput(ServerLauncher.class.getName());
    }

    /**
     * New Node will join the circle of trust based on its id and known existing node id
     * If this is the first node joining, existing id = 0
     *
     * @param port             RMI port of the new node
     * @param nodeId           id of the current node to join
     * @param existingNodeHost to fetch data from, none if current node is first
     * @param existingNodeId   to fetch data from, 0 if current node is first
     */
    public static void join(String host, int port, int nodeId, String existingNodeHost, int existingNodeId) {
        if (nodeLocal.isConnected()) {
            logger.warn("Cannot join without leaving first!");
            return;
        }
        try {
            nodeLocal.join(host, port, nodeId, existingNodeHost, existingNodeId);
        } catch (Exception e) {
            logger.error("RMI failed miserably", e);
            System.exit(1);
        }
    }

    /**
     * Current Node will leave the circle of trust
     */
    public static void leave() {
        if (!nodeLocal.isConnected()) {
            logger.warn("Cannot leave without joining first!");
            return;
        }
        try {
            nodeLocal.leave();
        } catch (Exception e) {
            logger.error("RMI failed miserably", e);
            System.exit(1);
        }
    }
}
