import it.unitn.ds.Replication;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.*;

/**
 * Test class for multithreading of the set of tasks:
 * - no can use ExecutorService.invokeAll() method because it waits until all the threads are finished
 * - can use CompletionService because it returns Future object from Callback as soon as it has been processed
 */
public class ThreadTest {

    public static void main(String[] args) {
        ExecutorService executorService = Executors.newFixedThreadPool(Replication.N);
        CompletionService<String> completionService = new ExecutorCompletionService<>(executorService);
        Set<Callable<String>> callable = new HashSet<>();
        callable.add(new Callable<String>() {
            @Override
            public String call() throws Exception {
                Thread.sleep(TimeUnit.SECONDS.toMillis(1));
                return "Task 1";
            }
        });
        callable.add(new Callable<String>() {
            @Override
            public String call() throws Exception {
                Thread.sleep(TimeUnit.SECONDS.toMillis(2));
                return "Task 2";
            }
        });
        callable.add(new Callable<String>() {
            @Override
            public String call() throws Exception {
                Thread.sleep(TimeUnit.SECONDS.toMillis(20));
                return "Task 3";
            }
        });
        for (Callable<String> call : callable) {
            completionService.submit(call);
        }
        executorService.shutdown();
        while (!executorService.isTerminated()) {
            try {
                Future<String> future = completionService.poll(Replication.TIMEOUT, TimeUnit.SECONDS);
                if (future == null) {
                    break;
                }
                System.out.println(future.get(Replication.TIMEOUT, TimeUnit.SECONDS));
            } catch (TimeoutException | InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
        }
        executorService.shutdownNow();
    }
}
