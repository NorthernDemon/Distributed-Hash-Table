package it.unitn.ds;

import it.unitn.ds.entity.Node;
import it.unitn.ds.rmi.NodeRemote;
import it.unitn.ds.rmi.NodeServer;
import it.unitn.ds.util.InputUtil;
import it.unitn.ds.util.RemoteUtil;
import it.unitn.ds.util.StorageUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.Nullable;

import java.rmi.Naming;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.util.Arrays;
import java.util.Map;

public final class ServerLauncher {

    private static final Logger logger = LogManager.getLogger();

    private static final int RMI_PORT = 1099;

    @Nullable
    private static Node node;

    /**
     * Description: method name,node host,node id,existing node host, existing node id
     * Example: join,localhost,10,none,0
     * Example: join,localhost,15,localhost,10
     * Example: join,localhost,20,localhost,15
     * Example: join,localhost,25,localhost,10
     * Example: leave
     */
    public static void main(String[] args) {
        logger.info("Server Node is ready for request >>");
        logger.info("Example: method name,node host,node id,existing node host, existing node id");
        logger.info("Example: join,localhost,10,localhost,0");
        logger.info("Example: join,localhost,15,localhost,10");
        logger.info("Example: join,localhost,20,localhost,15");
        logger.info("Example: join,localhost,25,localhost,10");
        logger.info("Example: leave");
        StorageUtil.init();
        InputUtil.readInput(ServerLauncher.class.getName());
    }

    /**
     * Signals current node to join the ring based on its id and known existing node id
     * If this is the first node joining, existing id = 0
     *
     * @param host             network host
     * @param nodeId           id for new current node
     * @param existingNodeHost to fetch data from, none if current node is first
     * @param existingNodeId   if of known existing node, or 0 if current node is the first
     * @throws Exception in case of RMI error
     */
    public static void join(String host, int nodeId, String existingNodeHost, int existingNodeId) throws Exception {
        if (nodeId <= 0) {
            logger.warn("Node id must be positive integer [ nodeID > 0 ] !");
            return;
        }
        if (isConnected()) {
            logger.warn("Cannot join without leaving first!");
            return;
        }
        startRMIRegistry();
        if (existingNodeId == 0) {
            logger.info("NodeId=" + nodeId + " is the first node in ring");
            node = register(host, nodeId);
            logger.info("NodeId=" + nodeId + " is connected as first node=" + node);
        } else {
            logger.info("NodeId=" + nodeId + " connects to existing nodeId=" + existingNodeId);
            NodeServer existingNode = RemoteUtil.getRemoteNode(existingNodeHost, existingNodeId, NodeServer.class);
            Map<Integer, String> existingNodes = existingNode.getNodes();
            if (existingNodes.containsKey(nodeId)) {
                logger.error("Cannot join as nodeId=" + nodeId + " already taken!");
                return;
            }
            Node successorNode = RemoteUtil.getSuccessorNode(nodeId, existingNodes);
            if (successorNode == null) {
                logger.error("Failed to register nodeId=" + nodeId + " as successor is not found");
                return;
            }
            node = register(host, nodeId);
            node.putNodes(existingNodes);
            announceJoin();
            RemoteUtil.transferItems(successorNode, node);
            RemoteUtil.transferReplicas(successorNode, node);
            logger.info("NodeId=" + nodeId + " connected as node=" + node + " with successorNode=" + successorNode);
        }
    }

    /**
     * Signals current node to leave the ring
     *
     * @throws Exception in case of RMI error
     */
    public static void leave() throws Exception {
        if (!isConnected()) {
            logger.warn("Cannot leave without joining first!");
            return;
        }
        logger.info("NodeId=" + node.getId() + " is disconnecting from the ring...");
        Node successorNode = RemoteUtil.getSuccessorNode(node);
        if (successorNode != null) {
            RemoteUtil.copyItems(node, successorNode);
        }
        RemoteUtil.removeReplicas(node);
        RemoteUtil.passReplicas(node);
        announceLeave();
        Naming.unbind(RemoteUtil.getRMI(node.getHost(), node.getId()));
        StorageUtil.removeFile(node.getId());
        logger.info("NodeId=" + node.getId() + " disconnected");
        node = null;
    }

    /**
     * Registers RMI for new node, initializes node object
     *
     * @param nodeId id of the current node to instantiate
     * @throws Exception of shutdown hook
     */
    private static Node register(String host, final int nodeId) throws Exception {
        System.setProperty("java.rmi.server.hostname", host);
        Node node = new Node(nodeId, host);
        Naming.bind(RemoteUtil.getRMI(host, nodeId), new NodeRemote(node));
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                logger.info("Auto-leaving nodeId=" + nodeId);
                try {
                    if (isConnected()) {
                        leave();
                    }
                } catch (Exception e) {
                    logger.error("Failed to leave node", e);
                }
            }
        });
        return node;
    }

    /**
     * Announce JOIN operation to the set of nodes
     */
    private static void announceJoin() throws RemoteException {
        logger.debug("NodeId=" + node.getId() + " announcing join to nodes=" + Arrays.toString(node.getNodes().entrySet().toArray()));
        for (int nodeId : node.getNodes().keySet()) {
            if (nodeId != node.getId()) {
                RemoteUtil.getRemoteNode(node.getNodes().get(nodeId), nodeId, NodeServer.class).addNode(node.getId(), node.getHost());
                logger.debug("NodeId=" + node.getId() + " announced join to nodeId=" + nodeId);
            }
        }
    }

    /**
     * Announce LEAVE operation to the set of known nodes
     */
    private static void announceLeave() throws RemoteException {
        logger.debug("NodeId=" + node.getId() + " announcing leave to nodes=" + Arrays.toString(node.getNodes().entrySet().toArray()));
        for (int nodeId : node.getNodes().keySet()) {
            if (nodeId != node.getId()) {
                RemoteUtil.getRemoteNode(node.getNodes().get(nodeId), nodeId, NodeServer.class).removeNode(node.getId());
                logger.debug("NodeId=" + node.getId() + " announced leave to nodeId=" + nodeId);
            }
        }
    }

    /**
     * Starts RMI registry on default port if not started already
     */
    private static void startRMIRegistry() {
        try {
            LocateRegistry.createRegistry(RMI_PORT);
        } catch (RemoteException e) {
            // already started
        }
    }

    private static boolean isConnected() {
        return node != null;
    }
}
