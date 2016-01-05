package org.openspaces.collections.queue.distributed;

import com.j_spaces.core.client.SQLQuery;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.openspaces.collections.queue.BasicQueueTest;
import org.openspaces.collections.queue.distributed.data.QueueItem;
import org.openspaces.collections.set.SerializableType;
import org.springframework.test.context.ContextConfiguration;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.openspaces.collections.CollectionUtils.createSerializableType;
import static org.openspaces.collections.CollectionUtils.createSerializableTypeList;

@RunWith(Parameterized.class)
@ContextConfiguration
public class DistributedQueueBasicTest extends BasicQueueTest<SerializableType> {

    public DistributedQueueBasicTest(List<SerializableType> elements) {
        super(elements);
    }

    @Parameterized.Parameters
    public static Collection<Object[]> testData() {
        return Arrays.asList(new Object[][]{
                {Collections.emptyList()},
                {Collections.singletonList(createSerializableType())},
                {createSerializableTypeList(10)},
//                { createSerializableTypeList(MEDIUM_COLLECTION_SIZE) },
        });
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
    protected SerializableType newElement() {
        return createSerializableType();
    }

    @Override
    protected void assertSize(String msg, int expectedSize) {
        SQLQuery<QueueItem> query = new SQLQuery<>(QueueItem.class, "itemKey.queueName = ?", queueName);
        assertEquals(msg, expectedSize, gigaSpace.count(query));
    }
}
