package it.unitn.ds.server;

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
import java.util.*;

public final class NodeLocal {

    private static final Logger logger = LogManager.getLogger();

    @Nullable
    private Node node;

    @Nullable
    private Registry registry;

    /**
     * Signals current node to join the circle of trust
     *
     * @param host             network host
     * @param nodeId           id for new current node
     * @param existingNodeHost to fetch data from, none if current node is first
     * @param existingNodeId   if of known existing node, or 0 if current node is the first
     * @throws Exception in case of RMI error
     */
    public void join(String host, int nodeId, String existingNodeHost, int existingNodeId) throws Exception {
        if (existingNodeId == 0) {
            logger.info("NodeId=" + nodeId + " is the first node in circle");
            node = register(host, nodeId, 1099);
            node.getNodes().put(node.getId(), node.getHost());
            logger.info("NodeId=" + nodeId + " is connected as first node=" + node);
        } else {
            logger.info("NodeId=" + nodeId + " connects to existing nodeId=" + existingNodeId);
            NodeRemote remoteNode = RemoteUtil.getRemoteNode(existingNodeHost, existingNodeId);
            if (remoteNode == null) {
                logger.warn("Cannot get remote nodeId=" + existingNodeId);
                return;
            }
            Map<Integer, String> nodes = remoteNode.getNodes();
            Node successorNode = getSuccessorNode(nodeId, nodes);
            node = register(host, nodeId, new Random().nextInt(10000) + 1100);
            node.getNodes().putAll(nodes);
            node.getNodes().put(node.getId(), node.getHost());
            announceJoin();
            if (successorNode != null) {
                transferItems(successorNode);
            }
            logger.info("NodeId=" + nodeId + " connected as node=" + node + " with successorNode=" + successorNode);
        }
    }

    /**
     * Signals current node to leave the circle of trust
     *
     * @throws Exception in case of RMI error
     */
    public void leave() throws Exception {
        logger.info("NodeId=" + node.getId() + " is disconnecting from the circle...");
        Node successorNode = getSuccessorNode(node.getId(), node.getNodes());
        if (successorNode != null) {
            copyItems(successorNode);
            announceLeave();
        }
        Naming.unbind(RemoteUtil.getRMI(node.getHost(), node.getId()));
        UnicastRemoteObject.unexportObject(registry, true);
        StorageUtil.removeFile(node.getId());
        logger.info("NodeId=" + node.getId() + " disconnected.");
        node = null;
        registry = null;
    }

    /**
     * Registers RMI for new node, initializes node object
     *
     * @param nodeId id of the current node to instantiate
     * @param port   RMI port
     * @throws Exception of shutdown hook
     */
    private Node register(String host, final int nodeId, int port) throws Exception {
        logger.debug("RMI registering with port=" + port);
        System.setProperty("java.rmi.server.hostname", host);
        registry = LocateRegistry.createRegistry(port);
        Node node = new Node(nodeId, host);
        Naming.bind(RemoteUtil.getRMI(host, nodeId), new NodeRemoteImpl(node));
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
     * Returns successor node clockwise in circle of the given node id in the set of nodes
     *
     * @param nodeId predecessor node id
     * @param nodes  set of possible successors
     * @return successor node of the given node id
     */
    @Nullable
    private Node getSuccessorNode(int nodeId, Map<Integer, String> nodes) throws RemoteException {
        int successorNodeId = getSuccessorNodeId(nodeId, nodes);
        if (successorNodeId == nodeId) {
            logger.debug("NodeId=" + nodeId + " did not find successorNode, except itself");
            return null;
        }
        logger.debug("NodeId=" + nodeId + " found successorNodeId=" + successorNodeId);
        NodeRemote remoteNode = RemoteUtil.getRemoteNode(nodes.get(successorNodeId), successorNodeId);
        if (remoteNode == null) {
            logger.warn("Cannot get remote nodeId=" + successorNodeId);
            return null;
        } else {
            return remoteNode.getNode();
        }
    }

    private int getSuccessorNodeId(int targetNodeId, Map<Integer, String> nodes) {
        for (int nodeId : nodes.keySet()) {
            if (nodeId > targetNodeId) {
                return nodeId;
            }
        }
        return nodes.keySet().iterator().next();
    }

    /**
     * Copies items from current node to successor node
     *
     * @param successorNode to which to transfer
     */
    private void copyItems(Node successorNode) throws RemoteException {
        if (node.getItems().isEmpty()) {
            logger.debug("Nothing to copy from node=" + node + " successorNode=" + successorNode);
            return;
        }
        NodeRemote remoteNode = RemoteUtil.getRemoteNode(successorNode.getHost(), successorNode.getId());
        if (remoteNode == null) {
            logger.warn("Cannot get remote nodeId=" + successorNode.getId());
            return;
        }
        remoteNode.updateItems(new ArrayList<>(node.getItems().values()));
        logger.debug("Copied items node=" + node + " successorNode=" + successorNode);
    }

    /**
     * Transfers items from successor node to current node, removes items of successorNode
     *
     * @param successorNode from which to transfer
     */
    private void transferItems(Node successorNode) throws RemoteException {
        if (successorNode.getItems().isEmpty()) {
            logger.debug("Nothing to transfer from successorNode=" + successorNode + " node=" + node);
            return;
        }
        List<Item> items = getTransferItems(successorNode);
        NodeRemote remoteNode = RemoteUtil.getRemoteNode(node.getHost(), node.getId());
        if (remoteNode == null) {
            logger.warn("Cannot get remote nodeId=" + node.getId());
            return;
        }
        remoteNode.updateItems(items);
        NodeRemote remoteSuccessor = RemoteUtil.getRemoteNode(successorNode.getHost(), successorNode.getId());
        if (remoteSuccessor == null) {
            logger.warn("Cannot get remote nodeId=" + successorNode.getId());
            return;
        }
        remoteSuccessor.removeItems(items);
        logger.debug("Transferred items successorNode=" + successorNode + " node=" + node + " items=" + Arrays.toString(items.toArray()));
    }

    /**
     * Returns a list of items, that should be transferred from successor node to current node
     * <p/>
     * If new node appears the last and successor it the first (zero-crossing)
     * it transfer only items between current node and its predecessor
     *
     * @param successorNode from which to transfer
     * @return list of items for current node
     */
    private List<Item> getTransferItems(Node successorNode) {
        boolean isZeroCrossed = successorNode.getId() < node.getId();
        List<Item> items = new ArrayList<>();
        for (Item item : successorNode.getItems().values()) {
            if (item.getKey() <= node.getId()) {
                // check if item (e.g. 5) falls in range of highest-identified node (e.g.20) or lowest (e.g. 5)
                if (isZeroCrossed) {
                    if (item.getKey() > successorNode.getId()) {
                        items.add(item);
                    }
                } else {
                    items.add(item);
                }
            } else {
                return items;
            }
        }
        return items;
    }

    /**
     * Announce JOIN operation to the set of nodes
     */
    private void announceJoin() throws RemoteException {
        logger.debug("NodeId=" + node.getId() + " announcing join to nodes=" + Arrays.toString(node.getNodes().entrySet().toArray()));
        for (int nodeId : node.getNodes().keySet()) {
            NodeRemote remoteNode = RemoteUtil.getRemoteNode(node.getNodes().get(nodeId), nodeId);
            if (remoteNode == null) {
                logger.warn("Cannot get remote nodeId=" + nodeId);
                continue;
            }
            remoteNode.addNode(node.getId(), node.getHost());
            logger.debug("NodeId=" + node.getId() + " announced join to nodeId=" + nodeId);
        }
    }

    /**
     * Announce LEAVE operation to the set of known nodes
     */
    private void announceLeave() throws RemoteException {
        logger.debug("NodeId=" + node.getId() + " announcing leave to nodes=" + Arrays.toString(node.getNodes().entrySet().toArray()));
        for (int nodeId : node.getNodes().keySet()) {
            if (nodeId != node.getId()) {
                NodeRemote remoteNode = RemoteUtil.getRemoteNode(node.getNodes().get(nodeId), nodeId);
                if (remoteNode == null) {
                    logger.warn("Cannot get remote nodeId=" + nodeId);
                    continue;
                }
                remoteNode.removeNode(node.getId());
                logger.debug("NodeId=" + node.getId() + " announced leave to nodeId=" + nodeId);
            }
        }
    }

    public boolean isConnected() {
        return node != null;
    }
}
