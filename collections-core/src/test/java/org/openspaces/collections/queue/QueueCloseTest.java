package org.openspaces.collections.queue;

import org.openspaces.collections.CollocationMode;
import org.openspaces.collections.GigaQueueConfigurer;
import org.openspaces.collections.set.SerializableType;
import org.openspaces.core.GigaSpace;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.*;

import java.lang.reflect.Method;
import java.util.EnumSet;
import java.util.Set;

import static org.openspaces.collections.util.TestUtils.singleParam;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

/**
 * @author Oleksiy_Dyagilev
 */
@ContextConfiguration("classpath:/partitioned-space-test-config.xml")
public class QueueCloseTest extends AbstractTestNGSpringContextTests {
    private static final Logger LOG = LoggerFactory.getLogger(QueueCloseTest.class);

    @DataProvider
    public static Object[][] queueTypes() {
        Object[][] types = singleParam(EnumSet.allOf(CollocationMode.class).toArray());
        LOG.info("Testing {} queue types", types.length);
        return types;
    }

    @Factory(dataProvider = "queueTypes")
    public static Object[] createTests(CollocationMode collocation) {
        return new Object[]{new QueueCloseTest(collocation)};
    }

    @Autowired
    protected GigaSpace gigaSpace;
    private CollocationMode collocation;

    public QueueCloseTest(CollocationMode collocation) {
        this.collocation = collocation;
    }

    @BeforeClass
    public void logCollocation() {
        LOG.info("Testing queue: collocation = {}", collocation);
    }

    @BeforeMethod
    public void before(Method method) {
        gigaSpace.clear(null);
        LOG.info("| running {}", method.getName());
    }

    public GigaBlockingQueue<SerializableType> createQueue() {
        String queueName = "test-close-queue-" + collocation.name().toLowerCase();
        return new GigaQueueConfigurer<SerializableType>(gigaSpace, queueName, collocation).gigaQueue();
    }

    @Test
    public void testQueueClose() throws Exception {
        GigaBlockingQueue<SerializableType> queue = createQueue();
        queue.offer(new SerializableType());
        queue.close();
        assertQueueClosed();
    }

    @Test(expectedExceptions = IllegalStateException.class)
    public void testOfferAfterClose() throws Exception {
        GigaBlockingQueue<SerializableType> queue = createQueue();
        queue.close();
        queue.offer(new SerializableType());
    }

    @Test(expectedExceptions = IllegalStateException.class)
    public void testPollAfterClose() throws Exception {
        GigaBlockingQueue<SerializableType> queue = createQueue();
        queue.offer(new SerializableType());
        queue.close();
        queue.poll();
    }

    private void assertQueueClosed() throws InterruptedException {
        assertEquals(gigaSpace.count(new Object()), 0);

        // make sure there is no running 'size change listener' thread
        if (checkQueueSizeListenerThread()) {
            // if it was in native call - give it a time, we cannot interrupt that call
            LOG.info("Waiting 5.5 seconds to check size change listener thread");
            Thread.sleep(5500);
            if (checkQueueSizeListenerThread()) {
                fail("found queue size check listener thread");
            }
        }
    }

    private boolean checkQueueSizeListenerThread() {
        Set<Thread> threadSet = Thread.getAllStackTraces().keySet();
        for (Thread thread : threadSet) {
            if (thread.getName().startsWith(AbstractGigaBlockingQueue.QUEUE_SIZE_CHANGE_LISTENER_THREAD_NAME)) {
                return true;
            }
        }
        return false;
    }

}