package org.openspaces.collections.queue;

import static org.junit.Assert.assertEquals;
import static org.openspaces.collections.CollectionUtils.MEDIUM_COLLECTION_SIZE;
import static org.openspaces.collections.CollectionUtils.createSerializableType;
import static org.openspaces.collections.CollectionUtils.createSerializableTypeList;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.junit.Before;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.openspaces.collections.CollocationMode;
import org.openspaces.collections.queue.data.QueueItem;
import org.openspaces.collections.set.SerializableType;
import org.springframework.test.context.ContextConfiguration;

import com.j_spaces.core.client.SQLQuery;

@RunWith(Parameterized.class)
@ContextConfiguration(locations = "classpath:/partitioned-space-test-config.xml")
public class DistributedQueueTest extends AbstractQueueTest<SerializableType> {

    private static final String QUEUE_NAME = "TestDistributedGigaBlockingQueue";
    
    public DistributedQueueTest(List<SerializableType> elements) {
        super(elements);
    }

    @Parameterized.Parameters
    public static Collection<Object[]> testData() {
        return Arrays.asList(new Object[][] { 
                { Collections.emptyList() },
                { Collections.singletonList(createSerializableType()) },
//                { createSerializableTypeList(MEDIUM_COLLECTION_SIZE) },
        });
    }

    @Before
    public void setUp() {
        this.gigaQueue = new DistributedGigaBlockingQueue<>(gigaSpace, QUEUE_NAME, CollocationMode.DISTRIBUTED);
        gigaQueue.addAll(testedElements);
    }
    
    @Override
    protected Class<? extends SerializableType> getElementType() {
        return SerializableType.class;
    }

    @Override
    protected SerializableType[] getElementArray() {
        return new SerializableType[testedElements.size()];
    }

    @Override
    protected SerializableType newNotNullElement() {
        return createSerializableType();
    }
    
    @Override
    protected void assertSize(String msg, int expectedSize) {
        SQLQuery<QueueItem> query = new SQLQuery<QueueItem>(QueueItem.class, "itemKey.queueName = ?", QUEUE_NAME);
        assertEquals(msg, expectedSize, gigaSpace.count(query));
    }
}
