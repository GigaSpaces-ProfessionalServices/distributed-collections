package org.openspaces.collections.queue;

import org.openspaces.collections.CollectionUtils;
import org.openspaces.collections.set.SerializableType;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.Test;

import javax.annotation.Resource;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

/**
 * @author Leonid_Poliakov
 */
@ContextConfiguration
public class QueueConfigurationTest extends AbstractTestNGSpringContextTests {
    @Resource(name = "myGigaQueue")
    private GigaBlockingQueue<SerializableType> queue;

    @Test
    public void test() {
        assertNotNull(queue);
        queue.add(CollectionUtils.createSerializableType());
        assertEquals(queue.size(), 1);
    }
}