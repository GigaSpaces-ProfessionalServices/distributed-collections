package org.openspaces.collections.queue;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.openspaces.collections.GigaQueueConfigurer;
import org.openspaces.collections.set.SerializableType;
import org.openspaces.core.GigaSpace;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.openspaces.collections.CollocationMode.DISTRIBUTED;

/**
 * @author Oleksiy_Dyagilev
 */
public abstract class QueueCloseTest {

    @Autowired
    protected GigaSpace gigaSpace;

    public abstract GigaBlockingQueue<SerializableType> createQueue();

    @Test
    public void testQueueClose() throws Exception {
        GigaBlockingQueue<SerializableType> queue = createQueue();
        queue.offer(new SerializableType());
        queue.close();
        assertQueueClosed();
    }

    @Before
    public void before() {
        gigaSpace.clear(null);
    }

    @Test(expected = IllegalStateException.class)
    public void testOfferAfterClose() throws Exception {
        GigaBlockingQueue<SerializableType> queue = createQueue();
        queue.close();
        queue.offer(new SerializableType());

    }

    @Test(expected = IllegalStateException.class)
    public void testPollAfterClose() throws Exception {
        GigaBlockingQueue<SerializableType> queue = createQueue();
        queue.offer(new SerializableType());
        queue.close();
        queue.poll();
    }

    private void assertQueueClosed() throws InterruptedException {
        assertEquals(0, gigaSpace.count(new Object()));

        //make sure there is no running 'size change listener' thread
        if (checkQueueSizeListenerThread()) {
            // if it was in native call - give it a time, we cannot interrupt that call
            System.out.println("waiting 5.5 seconds to check size change listener thread");
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
