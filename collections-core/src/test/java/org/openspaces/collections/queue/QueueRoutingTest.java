/**
 *
 */
package org.openspaces.collections.queue;

import com.j_spaces.core.client.SQLQuery;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.openspaces.collections.queue.distributed.data.QueueItem;
import org.openspaces.collections.set.SerializableType;
import org.openspaces.core.GigaSpace;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import javax.annotation.Resource;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.openspaces.collections.CollectionUtils.createSerializableType;
import static org.openspaces.collections.CollectionUtils.createSerializableTypeList;

/**
 * @author Svitlana_Pogrebna
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration
public class QueueRoutingTest {

    @Autowired
    private GigaSpace gigaSpace;
    @Resource
    private GigaBlockingQueue<SerializableType> gigaQueue;

    /** static reference to use junit @AfterClass **/
    private static GigaBlockingQueue gigaQueueStaticReference;

    @Value("${queue.name}")
    private String queueName;

    @Before
    public void setUp() {
        gigaQueueStaticReference = gigaQueue;
    }

    @After
    public void tearDown() {
        gigaSpace.clear(null);
    }

    @AfterClass
    public static void afterClass() throws Exception {
        System.out.println("Closing queue");
        gigaQueueStaticReference.close();
    }

    @Test
    public void testLocalModeRouting() {
        int count = 100;
        List<SerializableType> items = createSerializableTypeList(count);
        gigaQueue.addAll(items);
        checkRouting(count);

        SerializableType item1 = createSerializableType();
        gigaQueue.add(item1);
        checkRouting(++count);

        SerializableType item2 = createSerializableType();
        gigaQueue.offer(item2);
        checkRouting(++count);

        gigaQueue.removeAll(items);
        count -= items.size();
        checkRouting(count);

        gigaQueue.remove(item1);
        checkRouting(--count);

        gigaQueue.poll();
        checkRouting(--count);
    }

    private void checkRouting(int size) {
        SQLQuery<QueueItem> query = new SQLQuery<>(QueueItem.class, "itemKey.queueName = ?", queueName);
        QueueItem[] items = gigaSpace.readMultiple(query);
        assertNotNull("Items should not be null", items);
        assertEquals("Invalid items count", size, items.length);
        for (QueueItem item : items) {
            assertEquals("All itemKey should be routed to same partition", Integer.valueOf(queueName.hashCode()), item.getRouting());
        }
    }
}

