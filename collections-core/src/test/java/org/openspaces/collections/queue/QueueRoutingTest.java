/**
 *
 */
package org.openspaces.collections.queue;

import com.j_spaces.core.client.SQLQuery;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.openspaces.collections.queue.data.QueueItem;
import org.openspaces.collections.set.SerializableType;
import org.openspaces.core.GigaSpace;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import javax.annotation.Resource;
import java.util.List;

import static org.junit.Assert.*;
import static org.openspaces.collections.CollectionUtils.createSerializableType;
import static org.openspaces.collections.CollectionUtils.createSerializableTypeList;

/**
 * @author Svitlana_Pogrebna
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = "classpath:/gigaqueue-local-test-context.xml")
public class QueueRoutingTest {

    @Autowired
    private GigaSpace gigaSpace;
    @Resource
    private GigaBlockingQueue<SerializableType> gigaQueue;
    @Value("${queue.name}")
    private String queueName;

    @After
    public void tearDown() {
        gigaSpace.clear(null);
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

