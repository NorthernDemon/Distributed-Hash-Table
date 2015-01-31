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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.TreeSet;

public final class NodeLocal {

    private static final Logger logger = LogManager.getLogger();

    @Nullable
    private Node node;

    private Registry registry;

    /**
     * Signals current node to join the circle of trust
     *
     * @param port           RMI port
     * @param nodeId         id for new current node
     * @param existingNodeId if of known existing node, or 0 if current node is the first
     * @throws Exception in case of RMI error
     */
    public void join(int port, int nodeId, int existingNodeId) throws Exception {
        if (existingNodeId == 0) {
            logger.info("NodeId=" + nodeId + " is the first node in circle");
            register(nodeId, port);
            logger.info("NodeId=" + nodeId + " is connected as first node=" + node);
        } else {
            logger.info("NodeId=" + nodeId + " connects to existing nodeId=" + existingNodeId);
            TreeSet<Integer> nodes = RemoteUtil.getRemoteNode(existingNodeId).getNodes();
            Node successorNode = getSuccessorNode(nodeId, nodes);
            register(nodeId, port);
            announceJoin(successorNode.getNodes());
            transferItems(successorNode);
            logger.info("NodeId=" + nodeId + " connected as node=" + node + " with successorNode=" + successorNode);
        }
    }

    /**
     * Signals current node to leave the circle of trust
     *
     * @throws Exception in case of RMI error
     */
    public void leave() throws Exception {
        if (node == null) {
            logger.info("Node already left.");
            return;
        }
        logger.info("NodeId=" + node.getId() + " is disconnecting from the circle...");
        Node successorNode = getSuccessorNode(node.getId(), node.getNodes());
        if (successorNode != null) {
            copyItems(successorNode);
            announceLeave();
        }
        Naming.unbind(RemoteUtil.RMI_NODE + node.getId());
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
    private void register(final int nodeId, int port) throws Exception {
        logger.debug("RMI registering with port=" + port);
        registry = LocateRegistry.createRegistry(port);
        node = new Node(nodeId);
        node.getNodes().add(node.getId());
        Naming.bind(RemoteUtil.RMI_NODE + nodeId, new NodeRemoteImpl(node));
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                logger.info("Auto-leaving nodeId=" + nodeId);
                try {
                    leave();
                } catch (Exception e) {
                    logger.error("Failed to leave node", e);
                }
            }
        });
        logger.debug("RMI Node registered=" + node);
    }

    /**
     * Returns successor node clockwise in circle of the given node id in the set of nodes
     *
     * @param nodeId predecessor node id
     * @param nodes  set of possible successors
     * @return successor node of the given node id
     */
    @Nullable
    private Node getSuccessorNode(int nodeId, TreeSet<Integer> nodes) throws RemoteException {
        int successorNodeId = getSuccessorNodeId(nodeId, nodes);
        if (successorNodeId == nodeId) {
            logger.debug("NodeId=" + nodeId + " did not find successorNode, except itself");
            return null;
        }
        logger.debug("NodeId=" + nodeId + " found successorNodeId=" + successorNodeId);
        return RemoteUtil.getRemoteNode(successorNodeId).getNode();
    }

    private int getSuccessorNodeId(int targetNodeId, TreeSet<Integer> nodes) {
        for (int nodeId : nodes) {
            if (nodeId > targetNodeId) {
                return nodeId;
            }
        }
        return nodes.iterator().next();
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
        RemoteUtil.getRemoteNode(successorNode.getId()).updateItems(new ArrayList<>(node.getItems().values()));
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
        RemoteUtil.getRemoteNode(node.getId()).updateItems(items);
        RemoteUtil.getRemoteNode(successorNode.getId()).removeItems(items);
        logger.debug("Transferred items successorNode=" + successorNode + " node=" + node + " items=" + Arrays.toString(items.toArray()));
    }

    /**
     * Returns a list of items, that should be transferred from successor node to current node
     *
     * @param successorNode from which to transfer
     * @return list of items for current node
     */
    private List<Item> getTransferItems(Node successorNode) {
        boolean isZeroCrossed = successorNode.getId() < node.getId();
        List<Item> items = new ArrayList<>();
        for (Item item : successorNode.getItems().values()) {
            if (item.getKey() <= node.getId()) {
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
     * Announce JOIN operation to the set of nodes and updates internal set of current node
     *
     * @param nodes announcement receivers
     */
    private void announceJoin(TreeSet<Integer> nodes) throws RemoteException {
        logger.debug("NodeId=" + node.getId() + " announcing join to nodes=" + Arrays.toString(nodes.toArray()));
        for (int nodeId : nodes) {
            node.getNodes().add(nodeId);
            RemoteUtil.getRemoteNode(nodeId).addNode(node.getId());
            logger.debug("NodeId=" + node.getId() + " announced join to nodeId=" + nodeId);
        }
    }

    /**
     * Announce LEAVE operation to the set of known nodes
     */
    private void announceLeave() throws RemoteException {
        logger.debug("NodeId=" + node.getId() + " announcing leave to nodes=" + Arrays.toString(node.getNodes().toArray()));
        for (int nodeId : node.getNodes()) {
            if (nodeId != node.getId()) {
                RemoteUtil.getRemoteNode(nodeId).removeNode(node.getId());
                logger.debug("NodeId=" + node.getId() + " announced leave to nodeId=" + nodeId);
            }
        }
    }

    public boolean isConnected() {
        return node != null;
    }
}
