/**
 * 
 */
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
import org.junit.Ignore;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.openspaces.collections.queue.data.EmbeddedQueue;
import org.openspaces.collections.set.SerializableType;
import org.springframework.test.context.ContextConfiguration;

import com.j_spaces.core.client.SQLQuery;

/**
 * @author Svitlana_Pogrebna
 *
 */
@RunWith(Parameterized.class)
@ContextConfiguration(locations = "classpath:/partitioned-space-test-config.xml")
@Ignore
public class EmbeddedQueueTest extends AbstractQueueTest<SerializableType> {

    private static final String QUEUE_NAME = "TestEmbeddedGigaBlockingQueue";
    
    public EmbeddedQueueTest(List<SerializableType> elements) {
        super(elements);
    }

    @Parameterized.Parameters
    public static Collection<Object[]> testData() {
        return Arrays.asList(new Object[][] { 
                { Collections.emptyList() },
                { Collections.singletonList(createSerializableType()) },
                { createSerializableTypeList(MEDIUM_COLLECTION_SIZE) },
        });
    }

    @Before
    public void setUp() {
        this.gigaQueue = new EmbeddedGigaBlockingQueue<>(gigaSpace, QUEUE_NAME);
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
        SQLQuery<EmbeddedQueue> query = new SQLQuery<>(EmbeddedQueue.class, "name = ?", QUEUE_NAME).setProjections("container.size");
        int size = gigaSpace.read(query).getContainer().getSize();
        assertEquals(msg, expectedSize, size);
    }
}
