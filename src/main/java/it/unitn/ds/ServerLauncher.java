package it.unitn.ds;

import it.unitn.ds.entity.Item;
import it.unitn.ds.entity.Node;
import it.unitn.ds.rmi.NodeClient;
import it.unitn.ds.rmi.NodeRemote;
import it.unitn.ds.rmi.NodeServer;
import it.unitn.ds.rmi.NullNodeRemote;
import it.unitn.ds.util.InputUtil;
import it.unitn.ds.util.NetworkUtil;
import it.unitn.ds.util.RemoteUtil;
import it.unitn.ds.util.StorageUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.rmi.Naming;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Simulates server node in the ring
 *
 * @see it.unitn.ds.ClientLauncher
 */
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
     * Example: join,localhost,20,localhost,25
     * Example: crash
     * Example: recover,localhost,20
     * Example: leave
     */
    public static void main(String[] args) {
        if (Replication.W + Replication.R <= Replication.N) {
            logger.warn("Replication parameters must maintain formula [ W + R > N ] !");
            return;
        }
        logger.info("Server Node is ready for request >>");
        logger.info("Example: method name,node host,node id,existing node host, existing node id");
        logger.info("Example: join,localhost,10,localhost,0");
        logger.info("Example: join,localhost,15,localhost,10");
        logger.info("Example: join,localhost,20,localhost,15");
        logger.info("Example: join,localhost,25,localhost,20");
        logger.info("Example: join,localhost,30,localhost,25");
        logger.info("Example: crash");
        logger.info("Example: recover,localhost,20");
        logger.info("Example: leave");
        StorageUtil.init();
        NetworkUtil.printPossibleIPs();
        InputUtil.readInput(ServerLauncher.class.getName());
    }

    /**
     * Signals current node to join the ring and take items that fall into it's responsibility from the successor node
     *
     * @param nodeHost         host for new current node
     * @param nodeId           id for new current node
     * @param existingNodeHost of node in the ring to fetch data from, or 'none' if current node is the first
     * @param existingNodeId   of node in the ring to fetch data from, or 0 if current node is the first
     */
    public static void join(@NotNull String nodeHost, int nodeId, @NotNull String existingNodeHost, int existingNodeId) throws Exception {
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
            logger.info("NodeId=" + nodeId + " is the first node in the ring");
            node = register(nodeId, nodeHost);
            logger.info("NodeId=" + nodeId + " is connected as first node=" + node);
        } else {
            logger.info("NodeId=" + nodeId + " connects to existing nodeId=" + existingNodeId);
            NodeServer existingNode = RemoteUtil.getRemoteNode(new Node(existingNodeId, existingNodeHost), NodeServer.class);
            if (existingNode.getNodes().containsKey(nodeId)) {
                logger.error("Cannot join as nodeId=" + nodeId + " already taken!");
                return;
            }
            node = register(nodeId, nodeHost);
            node.putNodes(existingNode.getNodes());
            announceJoin();
            Node successorNode = RemoteUtil.getSuccessorNode(node);
            pickItems(successorNode);
            pickReplicas(successorNode);
            logger.info("NodeId=" + nodeId + " connected as node=" + node + " to successorNode=" + successorNode + " from existingNode=" + existingNode);
        }
        nodeState = NodeState.CONNECTED;
    }

    /**
     * Signals current node to leave the ring and pass all it's items to the successor node
     */
    public static void leave() throws Exception {
        if (nodeState != NodeState.CONNECTED) {
            logger.warn("Cannot leave without joining first!");
            return;
        }
        logger.info("NodeId=" + node.getId() + " is disconnecting from the ring...");
        passItems();
        passReplicas();
        announceLeave();
        Naming.unbind(RemoteUtil.getNodeRMI(node));
        StorageUtil.removeFile(node.getId());
        logger.info("NodeId=" + node.getId() + " disconnected");
        node = null;
        nodeState = NodeState.DISCONNECTED;
    }

    /**
     * Signals current node to crash, removes any in memory data, except for node id and host
     * Persistent storage (CSV file with items and replicas) remains untouched
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
     * @param existingNodeHost of node in the ring to fetch data from
     * @param existingNodeId   of node in the ring to fetch data from
     */
    public static void recover(@NotNull String existingNodeHost, int existingNodeId) throws Exception {
        if (nodeState != NodeState.CRASHED) {
            logger.warn("Cannot recover without crashing first!");
            return;
        }
        if (node.getId() == existingNodeId) {
            logger.warn("Existing node id must be different from crashed node id !");
            return;
        }
        logger.info("NodeId=" + node.getId() + " is recovering...");
        node.putNodes(RemoteUtil.getRemoteNode(new Node(existingNodeId, existingNodeHost), NodeServer.class).getNodes());
        Naming.rebind(RemoteUtil.getNodeRMI(node), new NodeRemote(node));
        distributeStorageItems();
        updateOwnItems();
        updateOwnReplicas();
        logger.info("NodeId=" + node.getId() + " has recovered");
        nodeState = NodeState.CONNECTED;
    }

    /**
     * Registers RMI for new node, initializes node object
     *
     * @param id   of the new node
     * @param host of the new node
     */
    @NotNull
    private static Node register(int id, @NotNull String host) throws Exception {
        System.setProperty("java.rmi.server.hostname", host);
        Node node = new Node(id, host);
        Naming.bind(RemoteUtil.getNodeRMI(node), new NodeRemote(node));
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
     * Picks items from successor node to current node
     *
     * @param successorNode from which to pick
     */
    private static void pickItems(@NotNull Node successorNode) throws RemoteException {
        List<Item> items = RemoteUtil.getNodeItems(node, successorNode);
        if (!items.isEmpty()) {
            Node nthSuccessor = RemoteUtil.getNthSuccessor(node, Replication.N);
            RemoteUtil.getRemoteNode(nthSuccessor, NodeServer.class).removeReplicas(items);
            logger.debug("Removed item replicas=" + Arrays.toString(items.toArray()) + " from nthSuccessor=" + nthSuccessor);
            RemoteUtil.getRemoteNode(node, NodeServer.class).updateItems(items);
            RemoteUtil.getRemoteNode(successorNode, NodeServer.class).removeItems(items);
            RemoteUtil.getRemoteNode(successorNode, NodeServer.class).updateReplicas(items);
            logger.debug("Picked items=" + Arrays.toString(items.toArray()) + " from successorNode=" + successorNode);
        }
    }

    /**
     * Picks replicas from successor node to current node
     *
     * @param successorNode from which to pick
     */
    private static void pickReplicas(@NotNull Node successorNode) throws RemoteException {
        if (isReplicaSmall()) {
            updateSmallReplicas(successorNode);
            return;
        }
        for (Item replica : successorNode.getReplicas().values()) {
            Node originalNode = RemoteUtil.getNodeForItem(replica.getKey(), node.getNodes());
            Node nthSuccessor = RemoteUtil.getNthSuccessor(originalNode, Replication.N);
            RemoteUtil.getRemoteNode(node, NodeServer.class).updateReplicas(Arrays.asList(replica));
            RemoteUtil.getRemoteNode(nthSuccessor, NodeServer.class).removeReplicas(Arrays.asList(replica));
            logger.debug("Picked replica=" + replica + " from nthSuccessor=" + nthSuccessor);
        }
    }

    /**
     * Passes current node items to its successor and updates replicas to new clockwise Nth successor
     */
    private static void passItems() throws RemoteException {
        if (!node.getItems().isEmpty()) {
            List<Item> items = new LinkedList<>(node.getItems().values());
            Node successorNode = RemoteUtil.getSuccessorNode(node);
            RemoteUtil.getRemoteNode(successorNode, NodeServer.class).removeReplicas(items);
            RemoteUtil.getRemoteNode(successorNode, NodeServer.class).updateItems(items);
            logger.debug("Passed items=" + Arrays.toString(items.toArray()) + " to successorNode=" + successorNode);
            if (isReplicaSmall()) {
                return;
            }
            Node nthSuccessor = RemoteUtil.getNthSuccessor(node, Replication.N);
            RemoteUtil.getRemoteNode(nthSuccessor, NodeServer.class).updateReplicas(items);
            logger.debug("Passed items replicas=" + Arrays.toString(items.toArray()) + " to nthSuccessor=" + nthSuccessor);
        }
    }

    /**
     * Passes replicas to other nodes
     */
    private static void passReplicas() throws RemoteException {
        if (isReplicaSmall()) {
            return;
        }
        for (Item replica : node.getReplicas().values()) {
            Node originalNode = RemoteUtil.getNodeForItem(replica.getKey(), node.getNodes());
            Node nthSuccessor = RemoteUtil.getNthSuccessor(originalNode, Replication.N);
            if (originalNode.getId() != nthSuccessor.getId()) {
                RemoteUtil.getRemoteNode(nthSuccessor, NodeServer.class).updateReplicas(Arrays.asList(replica));
                logger.debug("Passed replica=" + replica + " to nthSuccessor=" + nthSuccessor);
            }
        }
    }

    /**
     * Distributes items and replicas from local storage to respective nodes
     */
    private static void distributeStorageItems() throws RemoteException {
        for (Item item : StorageUtil.readAll(node.getId())) {
            Node nodeForItem = RemoteUtil.getNodeForItem(item.getKey(), node.getNodes());
            Item latestItem = RemoteUtil.getRemoteNode(nodeForItem, NodeClient.class).getItem(item.getKey());
            if (latestItem == null || latestItem.getVersion() <= item.getVersion()) {
                RemoteUtil.getRemoteNode(nodeForItem, NodeServer.class).updateItems(Arrays.asList(item));
                logger.debug("Distributed storage item=" + item + " to nodeForItem=" + nodeForItem);
                for (int i = 1; i < Replication.N; i++) {
                    if (!isReplicaSmall()) {
                        Node oldNthSuccessor = RemoteUtil.getNthSuccessor(node, i);
                        RemoteUtil.getRemoteNode(oldNthSuccessor, NodeServer.class).removeReplicas(Arrays.asList(item));
                        logger.debug("Removed storage replica item=" + item + " to oldNthSuccessor=" + oldNthSuccessor);
                    }
                    Node newNthSuccessor = RemoteUtil.getNthSuccessor(nodeForItem, i);
                    RemoteUtil.getRemoteNode(newNthSuccessor, NodeServer.class).updateReplicas(Arrays.asList(item));
                    logger.debug("Distributed storage replica item=" + item + " to newNthSuccessor=" + newNthSuccessor);
                }
            }
        }
    }

    /**
     * Updates own items of the current node from successor node
     */
    private static void updateOwnItems() throws RemoteException {
        Node successorNode = RemoteUtil.getSuccessorNode(node);
        for (Item replica : successorNode.getReplicas().values()) {
            Node nodeForItem = RemoteUtil.getNodeForItem(replica.getKey(), node.getNodes());
            if (nodeForItem.getId() == node.getId()) {
                RemoteUtil.getRemoteNode(node, NodeServer.class).updateItems(Arrays.asList(replica));
                logger.debug("Recovered replica=" + replica + " from successorNode=" + successorNode);
            }
        }
    }

    /**
     * Updates own replicas of the current node from predecessor node
     */
    private static void updateOwnReplicas() throws RemoteException {
        Node predecessorNode = RemoteUtil.getPredecessorNode(node);
        if (isReplicaSmall()) {
            updateSmallReplicas(predecessorNode);
            return;
        }
        for (Item replica : predecessorNode.getReplicas().values()) {
            Node nodeForItem = RemoteUtil.getNodeForItem(replica.getKey(), node.getNodes());
            for (int i = 1; i < Replication.N; i++) {
                Node nthSuccessor = RemoteUtil.getNthSuccessor(nodeForItem, i);
                if (nthSuccessor.getId() == node.getId()) {
                    RemoteUtil.getRemoteNode(node, NodeServer.class).updateReplicas(Arrays.asList(replica));
                    logger.debug("Recovered replica=" + replica + " from predecessorNode=" + predecessorNode);
                }
            }
        }
    }

    /**
     * If number of nodes less than replica nodes, assume all items to be replicas
     *
     * @param existingNode in the ring
     */
    private static void updateSmallReplicas(Node existingNode) throws RemoteException {
        List<Item> replicas = new LinkedList<>(existingNode.getItems().values());
        replicas.addAll(existingNode.getReplicas().values());
        replicas.removeAll(node.getItems().values());
        RemoteUtil.getRemoteNode(node, NodeServer.class).updateReplicas(replicas);
        logger.debug("[ replication <= nodes ] Picked replicas=" + Arrays.toString(replicas.toArray()) + " from existingNode=" + existingNode);
    }

    /**
     * Announce JOIN operation to the nodes in the ring
     */
    private static void announceJoin() throws RemoteException {
        logger.debug("Announcing join to nodes=" + Arrays.toString(node.getNodes().entrySet().toArray()));
        for (Map.Entry<Integer, String> entry : node.getNodes().entrySet()) {
            if (entry.getKey() != node.getId()) {
                RemoteUtil.getRemoteNode(new Node(entry.getKey(), entry.getValue()), NodeServer.class).addNode(node.getId(), node.getHost());
                logger.debug("Announced join to nodeId=" + entry.getKey());
            }
        }
    }

    /**
     * Announce LEAVE operation to the nodes in the ring
     */
    private static void announceLeave() throws RemoteException {
        logger.debug("Announcing leave to nodes=" + Arrays.toString(node.getNodes().entrySet().toArray()));
        for (Map.Entry<Integer, String> entry : node.getNodes().entrySet()) {
            if (entry.getKey() != node.getId()) {
                RemoteUtil.getRemoteNode(new Node(entry.getKey(), entry.getValue()), NodeServer.class).removeNode(node.getId());
                logger.debug("Announced leave to nodeId=" + entry.getKey());
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

    /**
     * Returns if number of nodes in the ring is smaller than replica size
     *
     * @return true if replica larger than nodes, false otherwise
     */
    private static boolean isReplicaSmall() {
        return node.getNodes().size() <= Replication.N;
    }
}
