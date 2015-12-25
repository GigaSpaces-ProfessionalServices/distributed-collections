/**
 * 
 */
package org.openspaces.collections.queue;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.openspaces.collections.CollectionUtils.createSerializableType;
import static org.openspaces.collections.CollectionUtils.createSerializableTypeList;

import java.util.List;

import javax.annotation.Resource;

import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.openspaces.collections.CollocationMode;
import org.openspaces.collections.queue.data.QueueItem;
import org.openspaces.collections.set.SerializableType;
import org.openspaces.core.GigaSpace;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.j_spaces.core.client.SQLQuery;

/**
 * @author Svitlana_Pogrebna
 *
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = "classpath:/partitioned-space-test-config.xml")
public class GigaBlockingQueueRoutingTest {

    private static final String QUEUE_NAME = "TestLocalGigaBlockingQueue";
    
    @Resource
    private GigaSpace gigaSpace;
    
    @After
    public void tearDown() {
        gigaSpace.clear(null);
    }
    
    @Test
    public void testLocalModeRouting() {
        GigaBlockingQueue<SerializableType> gigaQueue = new DefaultGigaBlockingQueue<>(gigaSpace, QUEUE_NAME, CollocationMode.LOCAL);
        
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
        SQLQuery<QueueItem> query = new SQLQuery<>(QueueItem.class, "itemKey.queueName = ?", QUEUE_NAME);
        QueueItem<SerializableType>[] items = gigaSpace.readMultiple(query);
        assertNotNull("Items should not be null", items);
        assertEquals("Invalid items count", size, items.length);
        for (QueueItem<SerializableType> item : items) {
            assertEquals("All itemKey should be routed to same partition", Integer.valueOf(QUEUE_NAME.hashCode()), item.getRouting());
        }
    }
}

