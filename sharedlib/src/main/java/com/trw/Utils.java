package com.trw;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * Created by tweissin on 3/10/17.
 */
public class Utils {
    /**
     * This tests that sending in requests
     */
    public static void testOrdering(int docCount, Function<Integer,Boolean> updateDocFunction) throws InterruptedException {
        ExecutorService executorService = Executors.newFixedThreadPool(docCount);

        for (int currentDoc=0; currentDoc<docCount; currentDoc++) {
            executorService.submit(new UpdateDoc(updateDocFunction, currentDoc));
        }
        executorService.shutdown();
        executorService.awaitTermination(1, TimeUnit.DAYS);
    }

    private static class UpdateDoc implements Callable<Boolean> {
        private Function<Integer, Boolean> updateDocFunction;
        private int currentDoc;

        UpdateDoc(Function<Integer,Boolean> updateDocFunction, int currentDoc) {
            this.updateDocFunction = updateDocFunction;
            this.currentDoc = currentDoc;
        }

        @Override
        public Boolean call() throws Exception {
            System.out.println("Updating doc " + currentDoc);
            updateDocFunction.apply(currentDoc);
            return true;
        }
    }
}
