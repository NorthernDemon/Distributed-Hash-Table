package it.unitn.ds;

import it.unitn.ds.entity.Item;
import it.unitn.ds.entity.Node;
import it.unitn.ds.rmi.NodeClient;
import it.unitn.ds.rmi.NodeRemote;
import it.unitn.ds.rmi.NodeServer;
import it.unitn.ds.rmi.NullNodeRemote;
import it.unitn.ds.util.InputUtil;
import it.unitn.ds.util.RemoteUtil;
import it.unitn.ds.util.StorageUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.Nullable;

import java.rmi.Naming;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public final class ServerLauncher {

    private static final Logger logger = LogManager.getLogger();

    private static final int RMI_PORT = 1099;

    @Nullable
    private static Node node;

    private static NodeState nodeState = NodeState.DISCONNECTED;

    /**
     * Description: method name,node host,node id,existing node host, existing node id
     * Example: join,localhost,10,none,0
     * Example: join,localhost,15,localhost,10
     * Example: join,localhost,20,localhost,15
     * Example: join,localhost,25,localhost,20
     * Example: crash
     * Example: recover,localhost,20
     * Example: leave
     */
    public static void main(String[] args) {
        logger.info("Server Node is ready for request >>");
        logger.info("Example: method name,node host,node id,existing node host, existing node id");
        logger.info("Example: join,localhost,10,localhost,0");
        logger.info("Example: join,localhost,15,localhost,10");
        logger.info("Example: join,localhost,20,localhost,15");
        logger.info("Example: join,localhost,25,localhost,20");
        logger.info("Example: crash");
        logger.info("Example: recover,localhost,20");
        logger.info("Example: leave");
        StorageUtil.init();
        InputUtil.readInput(ServerLauncher.class.getName());
    }

    /**
     * Signals current node to join the ring based on its id and known existing node id
     * If this is the first node joining, existing id = 0
     *
     * @param nodeHost         network node host
     * @param nodeId           id for new current node
     * @param existingNodeHost to fetch data from, none if current node is first
     * @param existingNodeId   if of known existing node, or 0 if current node is the first
     */
    public static void join(String nodeHost, int nodeId, String existingNodeHost, int existingNodeId) throws Exception {
        if (nodeState != NodeState.DISCONNECTED) {
            logger.warn("Cannot join without leaving first!");
            return;
        }
        if (nodeId <= 0) {
            logger.warn("Node id must be positive integer [ nodeID > 0 ] !");
            return;
        }
        startRMIRegistry();
        if (existingNodeId == 0) {
            logger.info("NodeId=" + nodeId + " is the first node in ring");
            node = register(nodeHost, nodeId);
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
            node = register(nodeHost, nodeId);
            node.putNodes(existingNodes);
            announceJoin();
            replicateItems(successorNode);
            logger.info("NodeId=" + nodeId + " connected as node=" + node + " with successorNode=" + successorNode);
        }
        nodeState = NodeState.CONNECTED;
    }

    /**
     * Signals current node to leave the ring
     */
    public static void leave() throws Exception {
        if (nodeState != NodeState.CONNECTED) {
            logger.warn("Cannot leave without joining first!");
            return;
        }
        logger.info("NodeId=" + node.getId() + " is disconnecting from the ring...");
        transferReplicas();
        Node successorNode = RemoteUtil.getSuccessorNode(node);
        if (successorNode != null) {
            List<Item> items = new ArrayList<>(node.getItems().values());
            if (!items.isEmpty()) {
                RemoteUtil.getRemoteNode(successorNode, NodeServer.class).removeReplicas(items);
                RemoteUtil.getRemoteNode(successorNode, NodeServer.class).updateItems(items);
                logger.debug("Copied items=" + Arrays.toString(items.toArray()) + " to successorNode=" + successorNode);
            }
        }
        announceLeave();
        Naming.unbind(RemoteUtil.getNodeRMI(node));
        StorageUtil.removeFile(node.getId());
        logger.info("NodeId=" + node.getId() + " disconnected");
        node = null;
        nodeState = NodeState.DISCONNECTED;
    }

    /**
     * Signals current node to crash, removes local knowledge of any in memory data, except for node id and host
     * Persistent storage (CSV file with items) remains untouched
     */
    public static void crash() throws Exception {
        if (nodeState != NodeState.CONNECTED) {
            logger.warn("Cannot crash without joining first!");
            return;
        }
        logger.info("NodeId=" + node.getId() + " is crashing down...");
        node = new Node(node);
        Naming.rebind(RemoteUtil.getNodeRMI(node), new NullNodeRemote(node));
        logger.info("NodeId=" + node.getId() + " has crashed");
        nodeState = NodeState.CRASHED;
    }

    /**
     * Signals current node to recover based on the existing node in the ring
     *
     * @param existingNodeHost to fetch data from
     * @param existingNodeId   if of known existing node
     */
    public static void recover(String existingNodeHost, int existingNodeId) throws Exception {
        if (nodeState != NodeState.CRASHED) {
            logger.warn("Cannot recover without crashing first!");
            return;
        }
        if (node.getId() == existingNodeId) {
            logger.warn("Existing node id must be different from crashed node id !");
            return;
        }
        logger.info("NodeId=" + node.getId() + " is recovering...");
        NodeServer existingNode = RemoteUtil.getRemoteNode(existingNodeHost, existingNodeId, NodeServer.class);
        Map<Integer, String> existingNodes = existingNode.getNodes();
        Node successorNode = RemoteUtil.getSuccessorNode(node.getId(), existingNodes);
        if (successorNode == null) {
            logger.error("Failed to recover nodeId=" + node.getId() + " as successor is not found");
            return;
        }
        node.putNodes(existingNodes);
        Naming.rebind(RemoteUtil.getNodeRMI(node), new NodeRemote(node));
        recoverItems(successorNode);
        logger.info("NodeId=" + node.getId() + " has recovered");
        nodeState = NodeState.CONNECTED;
    }

    private static void recoverItems(Node successorNode) throws RemoteException {
        // distribute local storage to other nodes
        for (Item item : StorageUtil.readAll(node.getId())) {
            Item latestItem = RemoteUtil.getRemoteNode(successorNode, NodeClient.class).getItem(item.getKey());
            Node nodeForItem = RemoteUtil.getNodeForItem(latestItem.getKey(), node.getNodes());
            RemoteUtil.getRemoteNode(nodeForItem, NodeServer.class).updateItems(Arrays.asList(latestItem));
            logger.debug("Recovered storage latestItem=" + latestItem + " to nodeForItem=" + nodeForItem);
            for (int i = 1; i < Replication.N; i++) {
                Node crashedNthSuccessor = RemoteUtil.getNthSuccessor(node.getId(), node.getNodes(), i);
                RemoteUtil.getRemoteNode(crashedNthSuccessor, NodeServer.class).removeReplicas(Arrays.asList(latestItem));
                Node originalNthSuccessor = RemoteUtil.getNthSuccessor(nodeForItem.getId(), node.getNodes(), i);
                RemoteUtil.getRemoteNode(originalNthSuccessor, NodeServer.class).updateReplicas(Arrays.asList(latestItem));
            }
        }
        // update own items based on successor node
        for (Item item : successorNode.getReplicas().values()) {
            Node nodeForItem = RemoteUtil.getNodeForItem(item.getKey(), node.getNodes());
            if (nodeForItem.getId() == node.getId()) {
                RemoteUtil.getRemoteNode(node, NodeServer.class).updateItems(Arrays.asList(item));
            } else {
                RemoteUtil.getRemoteNode(nodeForItem, NodeServer.class).updateItems(Arrays.asList(item));
                logger.debug("Recovered replica item=" + item + " to nodeForItem=" + nodeForItem);
                for (int i = 1; i < Replication.N; i++) {
                    Node crashedNthSuccessor = RemoteUtil.getNthSuccessor(node.getId(), node.getNodes(), i);
                    RemoteUtil.getRemoteNode(crashedNthSuccessor, NodeServer.class).removeReplicas(Arrays.asList(item));
                    Node originalNthSuccessor = RemoteUtil.getNthSuccessor(nodeForItem.getId(), node.getNodes(), i);
                    RemoteUtil.getRemoteNode(originalNthSuccessor, NodeServer.class).updateReplicas(Arrays.asList(item));
                }
            }
        }
    }

    /**
     * Registers RMI for new node, initializes node object
     *
     * @param id id of the current node to instantiate
     */
    private static Node register(String host, int id) throws Exception {
        System.setProperty("java.rmi.server.hostname", host);
        Node node = new Node(id, host);
        Naming.bind(RemoteUtil.getNodeRMI(host, id), new NodeRemote(node));
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                logger.info("Auto-leaving process initiated...");
                try {
                    if (nodeState == NodeState.CONNECTED) {
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
     * Replicates items from successor node to current node, removes items of fromNode
     *
     * @param successorNode from which to transfer
     */
    private static void replicateItems(Node successorNode) throws RemoteException {
        if (!successorNode.getItems().isEmpty()) {
            List<Item> items = getNodeItems(successorNode);
            RemoteUtil.getRemoteNode(node, NodeServer.class).updateItems(items);
            RemoteUtil.getRemoteNode(RemoteUtil.getNthSuccessor(node.getId(), node.getNodes(), Replication.N), NodeServer.class).removeReplicas(items);
            RemoteUtil.getRemoteNode(successorNode, NodeServer.class).removeItems(items);
            RemoteUtil.getRemoteNode(successorNode, NodeServer.class).updateReplicas(items);
            logger.debug("Transferred items=" + Arrays.toString(items.toArray()) + " from successorNode=" + successorNode);
        }
        List<Item> replicas = new ArrayList<>(successorNode.getReplicas().values());
        RemoteUtil.getRemoteNode(node, NodeServer.class).updateReplicas(replicas);
        for (Item replica : replicas) {
            Node originalNode = RemoteUtil.getNodeForItem(replica.getKey(), successorNode.getNodes());
            Node nthSuccessor = RemoteUtil.getNthSuccessor(originalNode.getId(), node.getNodes(), Replication.N);
            RemoteUtil.getRemoteNode(nthSuccessor, NodeServer.class).removeReplicas(Arrays.asList(replica));
            logger.debug("Removed replica=" + replica + " from node=" + nthSuccessor);
        }
        if (successorNode.getNodes().size() == 1) {
            List<Item> items = new ArrayList<>(successorNode.getItems().values());
            items.removeAll(node.getItems().values());
            RemoteUtil.getRemoteNode(node, NodeServer.class).updateReplicas(items);
        }
    }

    /**
     * Returns a list of items, that the given node is responsible for
     *
     * @param successorNode from which to find
     * @return list of items
     */
    private static List<Item> getNodeItems(Node successorNode) {
        int predecessorNodeId = RemoteUtil.getPredecessorNodeId(node);
        boolean isZeroCrossed = node.getId() < predecessorNodeId;
        List<Item> items = new ArrayList<>();
        for (Item item : successorNode.getItems().values()) {
            // check if item (e.g. 5) falls in range of highest-identified node (e.g. 20) or lowest (e.g. 5)
            if (isZeroCrossed && (item.getKey() <= node.getId() || item.getKey() > successorNode.getId())) {
                items.add(item);
            }
            if (!isZeroCrossed && item.getKey() <= node.getId() && item.getKey() > predecessorNodeId) {
                items.add(item);
            }
        }
        return items;
    }

    /**
     * Transfers replicas to other nodes when leaving
     */
    private static void transferReplicas() throws RemoteException {
        Node nthSuccessor = RemoteUtil.getNthSuccessor(node.getId(), node.getNodes(), Replication.N);
        if (node.getId() != nthSuccessor.getId()) {
            RemoteUtil.getRemoteNode(nthSuccessor, NodeServer.class).updateReplicas(new ArrayList<>(node.getItems().values()));
            logger.debug("Replicated item=" + Arrays.toString(node.getItems().keySet().toArray()) + " to nthSuccessor=" + nthSuccessor);
        }
        for (Item replica : node.getReplicas().values()) {
            Node originalNode = RemoteUtil.getNodeForItem(replica.getKey(), node.getNodes());
            nthSuccessor = RemoteUtil.getNthSuccessor(originalNode.getId(), node.getNodes(), Replication.N);
            if (originalNode.getId() != nthSuccessor.getId()) {
                RemoteUtil.getRemoteNode(nthSuccessor, NodeServer.class).updateReplicas(Arrays.asList(replica));
                logger.debug("Replicated replica=" + replica + " to nthSuccessor=" + nthSuccessor);
            }
        }
    }

    /**
     * Announce JOIN operation to the set of known nodes
     */
    private static void announceJoin() throws RemoteException {
        logger.debug("Announcing join to nodes=" + Arrays.toString(node.getNodes().entrySet().toArray()));
        for (int nodeId : node.getNodes().keySet()) {
            if (nodeId != node.getId()) {
                RemoteUtil.getRemoteNode(node.getNodes(), nodeId, NodeServer.class).addNode(node.getId(), node.getHost());
                logger.debug("Announced join to nodeId=" + nodeId);
            }
        }
    }

    /**
     * Announce LEAVE operation to the set of known nodes
     */
    private static void announceLeave() throws RemoteException {
        logger.debug("Announcing leave to nodes=" + Arrays.toString(node.getNodes().entrySet().toArray()));
        for (int nodeId : node.getNodes().keySet()) {
            if (nodeId != node.getId()) {
                RemoteUtil.getRemoteNode(node.getNodes(), nodeId, NodeServer.class).removeNode(node.getId());
                logger.debug("Announced leave to nodeId=" + nodeId);
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
}
