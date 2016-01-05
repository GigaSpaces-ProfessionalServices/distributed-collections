package org.openspaces.collections.queue.embedded;

import org.junit.runner.RunWith;
import org.openspaces.collections.GigaQueueConfigurer;
import org.openspaces.collections.queue.GigaBlockingQueue;
import org.openspaces.collections.queue.QueueCloseTest;
import org.openspaces.collections.set.SerializableType;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import static org.openspaces.collections.CollocationMode.EMBEDDED;

/**
 * @author Oleksiy_Dyagilev
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration
public class EmbeddedQueueCloseTest extends QueueCloseTest {

    @Override
    public GigaBlockingQueue<SerializableType> createQueue() {
        return new GigaQueueConfigurer<SerializableType>(gigaSpace, "TestClosingEmbeddedGigaBlockingQueue", EMBEDDED).gigaQueue();
    }
}
