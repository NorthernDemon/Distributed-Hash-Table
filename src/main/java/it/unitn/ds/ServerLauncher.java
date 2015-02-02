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
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.Arrays;
import java.util.Map;
import java.util.Random;

public final class ServerLauncher {

    private static final Logger logger = LogManager.getLogger();

    @Nullable
    private static Node node;

    @Nullable
    private static Registry registry;

    /**
     * ./server.jar {methodName},{host},{Own Node ID},{Existing Node ID or 0, if this is the first node}
     * <p/>
     * Example: join,localhost,10,none,0
     * Example: join,localhost,15,localhost,10
     * Example: leave
     */
    public static void main(String[] args) {
        logger.info("Server Node is ready for request>>");
        logger.info("Example: {methodName},{host},{Own Node ID},{Existing Node ID or 0, if this is the first node}");
        logger.info("Example: join,localhost,10,none,0");
        logger.info("Example: join,localhost,15,localhost,10");
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
        if (isConnected()) {
            logger.warn("Cannot join without leaving first!");
            return;
        }
        if (existingNodeId == 0) {
            logger.info("NodeId=" + nodeId + " is the first node in ring");
            LocateRegistry.createRegistry(1099);
            node = register(host, nodeId);
            logger.info("NodeId=" + nodeId + " is connected as first node=" + node);
        } else {
            logger.info("NodeId=" + nodeId + " connects to existing nodeId=" + existingNodeId);
            NodeServer remoteNode = RemoteUtil.getRemoteNode(existingNodeHost, existingNodeId, NodeServer.class);
            if (remoteNode == null) {
                logger.warn("Cannot get remote nodeId=" + existingNodeId);
                return;
            }
            Map<Integer, String> nodes = remoteNode.getNodes();
            Node successorNode = RemoteUtil.getSuccessorNode(nodeId, nodes);
            node = register(host, nodeId);
            node.putNodes(nodes);
            announceJoin();
            if (successorNode != null) {
                RemoteUtil.transferItems(successorNode, node);
            }
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
        Node successorNode = RemoteUtil.getSuccessorNode(node.getId(), node.getNodes());
        if (successorNode != null) {
            RemoteUtil.copyItems(node, successorNode);
            announceLeave();
        }
        Naming.unbind(RemoteUtil.getRMI(node.getHost(), node.getId()));
        UnicastRemoteObject.unexportObject(registry, true);
        StorageUtil.removeFile(node.getId());
        logger.info("NodeId=" + node.getId() + " disconnected");
        node = null;
        registry = null;
    }

    /**
     * Registers RMI for new node, initializes node object
     *
     * @param nodeId id of the current node to instantiate
     * @throws Exception of shutdown hook
     */
    private static Node register(String host, final int nodeId) throws Exception {
        int port = new Random().nextInt(10000) + 1100;
        logger.debug("RMI registering with port=" + port);
        System.setProperty("java.rmi.server.hostname", host);
        registry = LocateRegistry.createRegistry(port);
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
        logger.debug("RMI Node registered=" + node);
        return node;
    }

    /**
     * Announce JOIN operation to the set of nodes
     */
    private static void announceJoin() throws RemoteException {
        logger.debug("NodeId=" + node.getId() + " announcing join to nodes=" + Arrays.toString(node.getNodes().entrySet().toArray()));
        for (int nodeId : node.getNodes().keySet()) {
            if (nodeId != node.getId()) {
                NodeServer remoteNode = RemoteUtil.getRemoteNode(node.getNodes().get(nodeId), nodeId, NodeServer.class);
                if (remoteNode == null) {
                    logger.warn("Cannot get remote nodeId=" + nodeId);
                    continue;
                }
                remoteNode.addNode(node.getId(), node.getHost());
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
                NodeServer remoteNode = RemoteUtil.getRemoteNode(node.getNodes().get(nodeId), nodeId, NodeServer.class);
                if (remoteNode == null) {
                    logger.warn("Cannot get remote nodeId=" + nodeId);
                    continue;
                }
                remoteNode.removeNode(node.getId());
                logger.debug("NodeId=" + node.getId() + " announced leave to nodeId=" + nodeId);
            }
        }
    }

    private static boolean isConnected() {
        return node != null;
    }
}
