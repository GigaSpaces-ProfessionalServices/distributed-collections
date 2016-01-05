package org.openspaces.collections.queue;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.openspaces.core.GigaSpace;
import org.springframework.beans.factory.annotation.Autowired;

import javax.annotation.Resource;
import java.util.Random;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;

/**
 * @author Oleksiy_Dyagilev
 */
public abstract class ProducerConsumerTest {

    @Autowired
    protected GigaSpace gigaSpace;

    @Resource
    protected GigaBlockingQueue<Integer> gigaQueue;

    /** static reference to use junit @AfterClass **/
    private static GigaBlockingQueue gigaQueueStaticReference;

    private static final ExecutorService pool = Executors.newCachedThreadPool();

    private final AtomicInteger putSum = new AtomicInteger(0);
    private final AtomicInteger takeSum = new AtomicInteger(0);

    private final int nTrials = 1000;
    private final int nPairs = 10;
    private final CyclicBarrier barrier = new CyclicBarrier(nPairs * 2 + 1);

    @Before
    public void setUp() {
        gigaQueue.clear();
        gigaQueueStaticReference = gigaQueue;
    }

    @AfterClass
    public static void after() throws Exception {
        gigaQueueStaticReference.close();
    }

    @Test(timeout = 20000)
    public void testProducerConsumer() throws Exception {
        for (int i = 0; i < nPairs; i++) {
            pool.execute(new Producer());
            pool.execute(new Consumer());
        }

        barrier.await(); // wait for all threads to be ready
        barrier.await(); // wait for all threads to finish

        assertEquals(putSum.get(), takeSum.get());
    }

    class Producer implements Runnable {
        private Random random = ThreadLocalRandom.current();

        @Override
        public void run() {
            try {
                barrier.await(); // wait for all threads to be ready

                int sum = 0;
                for (int i = 0; i < nTrials; i++) {
                    int val = random.nextInt();
                    gigaQueue.put(val);
                    sum += val;
                }
                putSum.addAndGet(sum);

                barrier.await();  // wait for all threads to finish

            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    class Consumer implements Runnable {
        @Override
        public void run() {
            try {
                barrier.await();

                int sum = 0;
                for (int i = 0; i < nTrials; i++) {
                    sum += gigaQueue.take();
                }
                takeSum.addAndGet(sum);

                barrier.await();

            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

}
