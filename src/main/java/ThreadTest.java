import it.unitn.ds.Replication;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.*;

/**
 * Test class for multithreading of the set of tasks:
 *  - no can use ExecutorService.invokeAll() method because it waits until all the threads are finished
 *  - can use CompletionService because it returns Future object from Callback as soon as it has been processed
 *
 * http://pastie.org/private/wbivtgzrl5iofrjoccz2w
 */
public class ThreadTest {

    public static void main(String[] args) throws InterruptedException, ExecutionException {
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
                Thread.sleep(TimeUnit.SECONDS.toMillis(10));
                return "Task 3";
            }
        });
        for (Callable<String> call : callable) {
            completionService.submit(call);
        }
        executorService.shutdown();
        while (!executorService.isTerminated()) {
            Future<String> future = completionService.poll(Replication.TIMEOUT, TimeUnit.SECONDS);
            try {
                System.out.println(future.get(Replication.TIMEOUT, TimeUnit.SECONDS));
            } catch (Exception ce) {
                executorService.shutdownNow();
            }
        }
    }
}
