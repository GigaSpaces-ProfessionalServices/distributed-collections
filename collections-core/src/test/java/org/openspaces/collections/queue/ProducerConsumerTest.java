package org.openspaces.collections.queue;

import org.openspaces.collections.CollocationMode;
import org.openspaces.collections.GigaQueueConfigurer;
import org.openspaces.core.GigaSpace;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.*;

import java.lang.reflect.Method;
import java.util.EnumSet;
import java.util.Random;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

import static org.openspaces.collections.util.TestUtils.singleParam;
import static org.testng.Assert.assertEquals;

/**
 * @author Oleksiy_Dyagilev
 */
@ContextConfiguration("classpath:/partitioned-space-test-config.xml")
public class ProducerConsumerTest extends AbstractTestNGSpringContextTests {
    private static final Logger LOG = LoggerFactory.getLogger(ProducerConsumerTest.class);

    @DataProvider
    public static Object[][] queueTypes() {
        Object[][] types = singleParam(EnumSet.allOf(CollocationMode.class).toArray());
        LOG.info("Testing {} queue types", types.length);
        return types;
    }

    @Factory(dataProvider = "queueTypes")
    public static Object[] createTests(CollocationMode collocation) {
        return new Object[]{new ProducerConsumerTest(collocation)};
    }

    private static final ExecutorService pool = Executors.newCachedThreadPool();

    @Autowired
    protected GigaSpace gigaSpace;

    private CollocationMode collocation;
    protected GigaBlockingQueue<Integer> queue;

    private final AtomicInteger putSum = new AtomicInteger(0);
    private final AtomicInteger takeSum = new AtomicInteger(0);

    private final int nTrials = 1000;
    private final int nPairs = 10;
    private final CyclicBarrier barrier = new CyclicBarrier(nPairs * 2 + 1);

    public ProducerConsumerTest(CollocationMode collocation) {
        this.collocation = collocation;
    }

    @BeforeClass
    public void setUp() {
        LOG.info("Setting up queue: collocation = {}", collocation);
        String queueName = "test-producer-consumer-queue-" + collocation.name().toLowerCase();
        queue = new GigaQueueConfigurer<Integer>(gigaSpace, queueName, collocation).gigaQueue();
    }

    @BeforeMethod
    public void before(Method method) {
        LOG.info("| running {}", method.getName());
    }

    @AfterClass
    public void after() throws Exception {
        queue.close();
    }

    @Test(timeOut = 20000)
    public void testProducerConsumer() throws Exception {
        for (int i = 0; i < nPairs; i++) {
            pool.execute(new Producer());
            pool.execute(new Consumer());
        }

        barrier.await(); // wait for all threads to be ready
        barrier.await(); // wait for all threads to finish

        assertEquals(takeSum.get(), putSum.get());
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
                    queue.put(val);
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
                    sum += queue.take();
                }
                takeSum.addAndGet(sum);

                barrier.await();

            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }
}
