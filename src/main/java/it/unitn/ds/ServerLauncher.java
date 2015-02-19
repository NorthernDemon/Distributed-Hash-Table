package it.unitn.ds;

import it.unitn.ds.entity.Item;
import it.unitn.ds.entity.Node;
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
import java.util.*;

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
        NetworkUtil.printMachineIPv4();
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
            updateItemsAndReplicas();
            logger.info("NodeId=" + nodeId + " connected as node=" + node + " from existingNode=" + existingNode);
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
        passItemsAndReplicas();
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
        recoverItems();
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

    private static void updateItemsAndReplicas() throws RemoteException {
        Node successorNode = RemoteUtil.getSuccessorNode(node);
        List<Item> items = new ArrayList<>(getLatestItems(successorNode).values());
        if (!items.isEmpty()) {
            RemoteUtil.getRemoteNode(node, NodeServer.class).updateItems(items);
            RemoteUtil.getRemoteNode(successorNode, NodeServer.class).removeItems(items);
            RemoteUtil.getRemoteNode(successorNode, NodeServer.class).updateReplicas(items);
            logger.debug("Updated items=" + Arrays.toString(items.toArray()) + " from successorNode=" + successorNode);
            removerReplicaLastReplica(items);
        }
        List<Item> replicas = new ArrayList<>(getLatestReplicas());
        if (!replicas.isEmpty()) {
            RemoteUtil.getRemoteNode(node, NodeServer.class).updateReplicas(replicas);
            logger.debug("Updated replicas=" + Arrays.toString(replicas.toArray()) + " from successorNode=" + successorNode);
            removerReplicaLastReplica(replicas);
        }
    }

    private static void passItemsAndReplicas() throws RemoteException {
        List<Item> items = new ArrayList<>(getLatestItems(node).values());
        if (!items.isEmpty()) {
            Node successorNode = RemoteUtil.getSuccessorNode(node);
            RemoteUtil.getRemoteNode(successorNode, NodeServer.class).removeReplicas(items);
            RemoteUtil.getRemoteNode(successorNode, NodeServer.class).updateItems(items);
            logger.debug("Passed items=" + Arrays.toString(items.toArray()) + " to successorNode=" + successorNode);
            for (Item replica : node.getReplicas().values()) {
                Node nodeForItem = RemoteUtil.getNodeForItem(replica.getKey(), node.getNodes());
                Node nthSuccessor = RemoteUtil.getNthSuccessor(nodeForItem, node.getNodes(), Replication.N);
                RemoteUtil.getRemoteNode(nthSuccessor, NodeServer.class).updateReplicas(Arrays.asList(replica));
                logger.debug("Passed replica=" + replica + " to nthSuccessor=" + nthSuccessor);
            }
            Node nthSuccessor = RemoteUtil.getNthSuccessor(node, node.getNodes(), Replication.N);
            RemoteUtil.getRemoteNode(nthSuccessor, NodeServer.class).updateReplicas(items);
            logger.debug("Passed replicas=" + Arrays.toString(items.toArray()) + " to nthSuccessor=" + nthSuccessor);
        }
    }

    private static void removerReplicaLastReplica(@NotNull Collection<Item> items) throws RemoteException {
        if (node.getNodes().size() > Replication.N) {
            for (Item item : items) {
                Node nodeForItem = RemoteUtil.getNodeForItem(item.getKey(), node.getNodes());
                Node nthSuccessor = RemoteUtil.getNthSuccessor(nodeForItem, node.getNodes(), Replication.N);
                RemoteUtil.getRemoteNode(nthSuccessor, NodeServer.class).removeReplicas(Arrays.asList(item));
                logger.debug("Removed replica=" + item + " from nthSuccessor=" + nthSuccessor);
            }
        }
    }

    @NotNull
    private static Collection<Item> getLatestReplicas() throws RemoteException {
        Map<Integer, Item> replicas = new TreeMap<>();
        for (Item replica : RemoteUtil.getSuccessorNode(node).getReplicas().values()) {
            putItemIfNewer(replicas, replica);
        }
        Node predecessorNode = node;
        for (int i = 1; i < Replication.N; i++) {
            predecessorNode = RemoteUtil.getPredecessorNode(predecessorNode);
            for (Item item : predecessorNode.getItems().values()) {
                putItemIfNewer(replicas, item);
            }
            for (Item replica : predecessorNode.getReplicas().values()) {
                int nodeIdForItem = RemoteUtil.getNodeIdForItem(replica.getKey(), node.getNodes());
                for (int j = 1; j < Replication.N; j++) {
                    nodeIdForItem = RemoteUtil.getSuccessorNodeId(nodeIdForItem, node.getNodes());
                    if (node.getId() == nodeIdForItem) {
                        putItemIfNewer(replicas, replica);
                        break;
                    }
                }
            }
        }
        return replicas.values();
    }

    @NotNull
    private static Map<Integer, Item> getLatestItems(@NotNull Node startNode) throws RemoteException {
        Map<Integer, Item> items = new TreeMap<>();
        int predecessorNodeId = RemoteUtil.getPredecessorNodeId(node);
        putItems(predecessorNodeId, items, startNode.getItems());
        for (int i = 1; i < Replication.N; i++) {
            Node nthSuccessor = RemoteUtil.getNthSuccessor(startNode, node.getNodes(), i);
            putItems(predecessorNodeId, items, nthSuccessor.getReplicas());
        }
        return items;
    }

    private static void putItems(int predecessorNodeId, @NotNull Map<Integer, Item> items, @NotNull Map<Integer, Item> nodeItems) {
        for (Item item : nodeItems.values()) {
            if (node.getId() < predecessorNodeId) {
                // zero crossed (e.g. node 10 has predecessor 30, holds items 8 and 36)
                if (item.getKey() <= node.getId() || item.getKey() > predecessorNodeId) {
                    putItemIfNewer(items, item);
                }
            } else {
                if (item.getKey() <= node.getId() && item.getKey() > predecessorNodeId) {
                    putItemIfNewer(items, item);
                }
            }
        }
    }

    private static void putItemIfNewer(@NotNull Map<Integer, Item> items, @NotNull Item item) {
        Item existingItem = items.get(item.getKey());
        if (existingItem == null || existingItem.getVersion() < item.getVersion()) {
            items.put(item.getKey(), item);
        }
    }

    /**
     * Distributes items and replicas from local storage to respective nodes if our version is newer than in the ring
     */
    private static void recoverItems() throws RemoteException {
        List<Item> localStorage = StorageUtil.readAll(node.getId());
        Node successorNode = RemoteUtil.getSuccessorNode(node);
        List<Item> items = new ArrayList<>(getLatestItems(successorNode).values());
        RemoteUtil.getRemoteNode(node, NodeServer.class).updateItems(items);
        logger.debug("Recovered items=" + Arrays.toString(items.toArray()));
        List<Item> replicas = new ArrayList<>(getLatestReplicas());
        RemoteUtil.getRemoteNode(node, NodeServer.class).updateReplicas(replicas);
        logger.debug("Recovered replicas=" + Arrays.toString(replicas.toArray()));
        for (Item item : localStorage) {
            Node nodeForItem = RemoteUtil.getNodeForItem(item.getKey(), node.getNodes());
            Item latestItems = getLatestNodeItem(nodeForItem, item.getKey());
            if (latestItems == null || item.getVersion() > latestItems.getVersion()) {
                RemoteUtil.getRemoteNode(nodeForItem, NodeServer.class).updateItems(Arrays.asList(item));
                logger.debug("Recovered storage item=" + item + " to nodeForItem=" + nodeForItem);
                for (int i = 1; i < Replication.N; i++) {
                    Node nthSuccessor = RemoteUtil.getNthSuccessor(nodeForItem, node.getNodes(), i);
                    RemoteUtil.getRemoteNode(nthSuccessor, NodeServer.class).updateReplicas(Arrays.asList(item));
                    logger.debug("Recovered storage replica=" + item + " to nthSuccessor=" + nthSuccessor);
                }
            }
        }
    }

    @Nullable
    private static Item getLatestNodeItem(Node nodeForItem, int itemKey) throws RemoteException {
        Map<Integer, Item> items = new TreeMap<>();
        for (Item item : nodeForItem.getItems().values()) {
            putItemIfNewer(items, item);
        }
        for (int i = 1; i < Replication.N; i++) {
            for (Item item : RemoteUtil.getNthSuccessor(nodeForItem, node.getNodes(), i).getReplicas().values()) {
                putItemIfNewer(items, item);
            }
        }
        return items.get(itemKey);
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
}
