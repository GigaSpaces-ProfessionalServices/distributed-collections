package org.openspaces.collections.queue;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.openspaces.collections.CollocationMode;
import org.openspaces.collections.serialization.DefaultSerializerProvider;
import org.openspaces.collections.serialization.ElementSerializer;
import org.openspaces.collections.set.SerializableType;
import org.openspaces.core.GigaSpace;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.openspaces.collections.CollectionUtils.createSerializableType;
import static org.openspaces.collections.CollectionUtils.createSerializableTypeList;

/**
 * @author Oleksiy_Dyagilev
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = "classpath:/partitioned-space-test-config.xml")
public class QueueCloseTest {

    @Autowired
    private GigaSpace gigaSpace;

    @Test
    public void testQueueClose() throws Exception {
        DistributedGigaBlockingQueue<SerializableType> queue = createDistributedQueue();
        queue.offer(new SerializableType());
        queue.close();
        assertQueueClosed();
    }


    @Test(expected = IllegalStateException.class)
    public void testOfferAfterClose() throws Exception {
        DistributedGigaBlockingQueue<SerializableType> queue = createDistributedQueue();
        queue.close();
        queue.offer(new SerializableType());

    }

    private DistributedGigaBlockingQueue<SerializableType> createDistributedQueue() {
        ElementSerializer serializer = new DefaultSerializerProvider().pickSerializer(SerializableType.class);
        return new DistributedGigaBlockingQueue<>(gigaSpace, "test-queue", 100, CollocationMode.DISTRIBUTED, serializer);
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
