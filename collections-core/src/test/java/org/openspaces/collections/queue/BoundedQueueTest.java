package org.openspaces.collections.queue;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.openspaces.collections.set.SerializableType;
import org.openspaces.core.GigaSpace;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import javax.annotation.Resource;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;
import static org.openspaces.collections.CollectionUtils.createSerializableType;
import static org.openspaces.collections.CollectionUtils.createSerializableTypeList;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = "classpath:/gigaqueue-bounded-test-context.xml")
@Ignore
public class BoundedQueueTest {

    private static final long TIMEOUT = 1000; // in milliseconds
    private static final long TIMEOUT_ACCURACY = 10; // in milliseconds

    @Value("${queue.capacity}")
    private int capacity;

    @Autowired
    private GigaSpace gigaSpace;
    @Resource
    private GigaBlockingQueue<SerializableType> gigaQueue;

    @Before
    public void setUp() {
        gigaQueue.addAll(createSerializableTypeList(capacity));
    }

    @After
    public void tearDown() {
        gigaSpace.clear(null);
    }

    @Test(expected = IllegalStateException.class)
    public void testAddToFullQueue() {
        gigaQueue.add(createSerializableType());
    }

    @Test(expected = IllegalStateException.class)
    public void testAddAllToFullQueue() {
        final int count = 200;
        gigaQueue.addAll(createSerializableTypeList(count));
    }

    @Test
    public void testOfferToFullQueue() {
        assertFalse("The element should not be inserted due to reaching the capacity", gigaQueue.offer(createSerializableType()));
    }

    @Test
    public void testRemainingCapacity() {
        assertEquals("Invalid remaining capacity", 0, gigaQueue.remainingCapacity());

        gigaQueue.clear();
        int expectedCapacity = capacity;
        assertEquals("Invalid remaining capacity", expectedCapacity, gigaQueue.remainingCapacity());

        SerializableType element = createSerializableType();
        assertTrue(gigaQueue.add(element));
        assertEquals("Invalid remaining capacity", --expectedCapacity, gigaQueue.remainingCapacity());

        assertTrue(gigaQueue.remove(element));
        assertEquals("Invalid remaining capacity", ++expectedCapacity, gigaQueue.remainingCapacity());

        final int count = 50;
        List<SerializableType> elements = createSerializableTypeList(count);
        assertTrue(gigaQueue.addAll(elements));

        expectedCapacity -= count;
        assertEquals("Invalid remaining capacity", expectedCapacity, gigaQueue.remainingCapacity());

        assertTrue(gigaQueue.removeAll(elements));
        expectedCapacity += count;
        assertEquals("Invalid remaining capacity", expectedCapacity, gigaQueue.remainingCapacity());
    }

    @Test(timeout = TIMEOUT)
    public void testOfferWithTimeoutFullQueue() throws InterruptedException {
        gigaQueue.offer(createSerializableType(), TIMEOUT - TIMEOUT_ACCURACY, TimeUnit.MILLISECONDS);
    }
}
