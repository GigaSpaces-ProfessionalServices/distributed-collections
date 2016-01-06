package org.openspaces.collections.queue;

import com.j_spaces.core.client.SQLQuery;
import org.openspaces.collections.GigaQueueConfigurer;
import org.openspaces.collections.queue.distributed.data.DistrQueueItem;
import org.openspaces.collections.set.SerializableType;
import org.openspaces.core.GigaSpace;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.List;

import static org.openspaces.collections.CollectionUtils.createSerializableType;
import static org.openspaces.collections.CollectionUtils.createSerializableTypeList;
import static org.openspaces.collections.CollocationMode.LOCAL;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

/**
 * @author Svitlana_Pogrebna
 */
@ContextConfiguration("classpath:/partitioned-space-test-config.xml")
public class LocalQueueRoutingTest extends AbstractTestNGSpringContextTests {
    private static final Logger LOG = LoggerFactory.getLogger(LocalQueueRoutingTest.class);

    @Autowired
    private GigaSpace gigaSpace;
    private GigaBlockingQueue<SerializableType> queue;
    private String queueName = "test-local-queue-routing";

    @BeforeClass
    public void setUp() {
        queue = new GigaQueueConfigurer<SerializableType>(gigaSpace, queueName, LOCAL).gigaQueue();
    }

    @AfterMethod
    public void tearDown() {
        gigaSpace.clear(null);
    }

    @AfterClass
    public void afterClass() throws Exception {
        System.out.println("Closing queue");
        queue.close();
    }

    @Test
    public void testLocalModeRouting() {
        int count = 100;
        List<SerializableType> items = createSerializableTypeList(count);
        queue.addAll(items);
        checkRouting(count);

        SerializableType item1 = createSerializableType();
        queue.add(item1);
        checkRouting(++count);

        SerializableType item2 = createSerializableType();
        queue.offer(item2);
        checkRouting(++count);

        queue.removeAll(items);
        count -= items.size();
        checkRouting(count);

        queue.remove(item1);
        checkRouting(--count);

        queue.poll();
        checkRouting(--count);
    }

    private void checkRouting(int size) {
        SQLQuery<DistrQueueItem> query = new SQLQuery<>(DistrQueueItem.class, "itemKey.queueName = ?", queueName);
        DistrQueueItem[] items = gigaSpace.readMultiple(query);
        assertNotNull(items, "Items should not be null");
        assertEquals(items.length, size, "Invalid items count");
        for (DistrQueueItem item : items) {
            assertEquals(item.getRouting(), Integer.valueOf(queueName.hashCode()), "All itemKey should be routed to same partition");
        }
    }
}