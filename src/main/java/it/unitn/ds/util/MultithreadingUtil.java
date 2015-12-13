package it.unitn.ds.util;

import it.unitn.ds.Replication;
import it.unitn.ds.entity.Item;
import it.unitn.ds.entity.Node;
import it.unitn.ds.rmi.NodeServer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;

import java.rmi.RemoteException;
import java.util.*;
import java.util.concurrent.*;

/**
 * Convenient class to work with multithreading form replicas requests
 *
 * @see it.unitn.ds.Replication
 * @see java.util.concurrent.ExecutorService
 * @see java.util.concurrent.CompletionService
 */
public abstract class MultithreadingUtil {

    private static final Logger logger = LogManager.getLogger();

    /**
     * Asynchronous update of the replicas (excluding item on original node) served by Replication.N - 1 threads
     *
     * @param item        to update
     * @param nodeForItem original node of the item
     * @param nodes       set of nodes
     * @see it.unitn.ds.Replication
     */
    public static void updateReplicas(@NotNull final Item item, @NotNull final Node nodeForItem, @NotNull final Map<Integer, String> nodes) {
        ExecutorService executorService = Executors.newFixedThreadPool(Replication.N - 1);
        for (int i = 1; i < Replication.N; i++) {
            final int finalI = i;
            executorService.execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        Node nthSuccessor = RemoteUtil.getNthSuccessor(nodeForItem, nodes, finalI);
                        if (nthSuccessor.getId() != nodeForItem.getId()) {
                            RemoteUtil.getRemoteNode(nthSuccessor, NodeServer.class).updateReplicas(Arrays.asList(item));
                            logger.debug("Replicated item=" + item + " to nthSuccessor=" + nthSuccessor);
                        }
                    } catch (RemoteException e) {
                        logger.error("Failed to get node via RMI", e);
                    }
                }
            });
        }
    }

    /**
     * Synchronous request for replicas (excluding item on the original node) served by Replication.N - 1 threads
     * <p>
     * Uses non-waiting CompletionService interface, which returns Future object from Callback as soon as it has been processed
     *
     * @param itemKey               of the item
     * @param nodeForItem           original node of the item
     * @param isOriginalOperational true if original node has non-null item, false otherwise
     * @param nodes                 set of nodes
     * @return collection of replicas
     * @see it.unitn.ds.Replication
     */
    @NotNull
    public static List<Item> getReplicas(int itemKey, @NotNull Node nodeForItem, boolean isOriginalOperational, Map<Integer, String> nodes) {
        ExecutorService executorService = Executors.newFixedThreadPool(Replication.N - 1);
        CompletionService<Item> completionService = new ExecutorCompletionService<>(executorService);
        for (Callable<Item> callable : getReadCallables(itemKey, nodeForItem, nodes)) {
            completionService.submit(callable);
        }
        executorService.shutdown();
        return getReplicasFast(isOriginalOperational ? Replication.R - 1 : Replication.R, executorService, completionService);
    }

    /**
     * Returns a set of Callable objects with replica request
     *
     * @param itemKey     of the item
     * @param nodeForItem original node of the item
     * @param nodes       set of nodes
     * @return set of Callable objects with replica request
     */
    @NotNull
    private static Set<Callable<Item>> getReadCallables(final int itemKey, @NotNull final Node nodeForItem, final Map<Integer, String> nodes) {
        Set<Callable<Item>> callable = new HashSet<>();
        for (int i = 1; i < Replication.N; i++) {
            final int finalI = i;
            callable.add(new Callable<Item>() {
                @Override
                public Item call() throws Exception {
                    Node nthSuccessor = RemoteUtil.getNthSuccessor(nodeForItem, nodes, finalI);
                    Item replica = nthSuccessor.getReplicas().get(itemKey);
                    logger.debug("Got replica=" + replica + " from nthSuccessor=" + nthSuccessor);
                    return replica;
                }
            });
        }
        return callable;
    }

    /**
     * Executes the set of Callable replica requests concurrently and returns a collection of replicas within TIMEOUT in SECONDS
     *
     * @param countReplicas     minimal number of replicas, sufficient for request,
     *                          method will return collection of items as soon as
     *                          this amount of items has been received from replicas
     * @param executorService   of thread executor
     * @param completionService of non-waiting thread executor
     * @return collection of replicas within TIMEOUT in SECONDS
     * @see it.unitn.ds.Replication
     */
    @NotNull
    private static List<Item> getReplicasFast(int countReplicas, @NotNull ExecutorService executorService, @NotNull CompletionService<Item> completionService) {
        List<Item> replicas = new LinkedList<>();
        while (!executorService.isTerminated()) {
            try {
                Future<Item> future = completionService.poll(Replication.TIMEOUT.getValue(), Replication.TIMEOUT.getUnit());
                if (future == null) {
                    break; // timeout
                }
                Item replica = future.get(Replication.TIMEOUT.getValue(), Replication.TIMEOUT.getUnit());
                if (replica != null) {
                    replicas.add(replica);
                    if (replicas.size() == countReplicas) {
                        break; // enough replicas responded
                    }
                }
            } catch (Exception e) {
                logger.error("Failed to execute the thread", e);
            }
        }
        executorService.shutdownNow();
        return replicas;
    }
}
