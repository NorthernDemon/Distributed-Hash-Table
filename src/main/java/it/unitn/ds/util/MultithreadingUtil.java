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
 * Convenient class to work with multithreading of replicas requests
 */
public abstract class MultithreadingUtil {

    private static final Logger logger = LogManager.getLogger();

    public static void updateReplicas(@NotNull final Item item, @NotNull final Node nodeForItem, @NotNull final Map<Integer, String> nodes) throws RemoteException {
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
                        logger.error("RMI failed miserably", e);
                    }
                }
            });
        }
    }

    @NotNull
    public static List<Item> getReplicas(int itemKey, @NotNull Node nodeForItem, int countExistingItems, Map<Integer, String> nodes) {
        ExecutorService executorService = Executors.newFixedThreadPool(Replication.N - 1);
        CompletionService<Item> completionService = new ExecutorCompletionService<>(executorService);
        for (Callable<Item> callable : getReadCallables(itemKey, nodeForItem, nodes)) {
            completionService.submit(callable);
        }
        executorService.shutdown();
        return getReplicasFast(Replication.R - countExistingItems, executorService, completionService);
    }

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

    @NotNull
    private static List<Item> getReplicasFast(int countReplicas, @NotNull ExecutorService executorService, @NotNull CompletionService<Item> completionService) {
        List<Item> replicas = new LinkedList<>();
        while (!executorService.isTerminated()) {
            try {
                Future<Item> future = completionService.poll(Replication.TIMEOUT, TimeUnit.SECONDS);
                Item replica = future.get(Replication.TIMEOUT, TimeUnit.SECONDS);
                if (replica != null) {
                    replicas.add(replica);
                    if (replicas.size() == countReplicas) {
                        return replicas;
                    }
                }
            } catch (Exception ce) {
                executorService.shutdownNow();
            }
        }
        return replicas;
    }
}
