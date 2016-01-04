/**
 *
 */
package org.openspaces.collections.queue.embedded;

import com.j_spaces.core.client.SQLQuery;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.openspaces.collections.queue.BasicQueueTest;
import org.openspaces.collections.queue.embedded.data.EmbeddedQueue;
import org.openspaces.collections.set.SerializableType;
import org.springframework.test.context.ContextConfiguration;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.openspaces.collections.CollectionUtils.*;

/**
 * @author Svitlana_Pogrebna
 */
@RunWith(Parameterized.class)
@ContextConfiguration
public class EmbeddedQueueBasicTest extends BasicQueueTest<SerializableType> {

    public EmbeddedQueueBasicTest(List<SerializableType> elements) {
        super(elements);
    }

    @Parameterized.Parameters
    public static Collection<Object[]> testData() {
        return Arrays.asList(new Object[][]{
                {Collections.emptyList()},
                {Collections.singletonList(createSerializableType())},
                {createSerializableTypeList(MEDIUM_COLLECTION_SIZE)},
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
        SQLQuery<EmbeddedQueue> query = new SQLQuery<>(EmbeddedQueue.class, "name = ?", queueName);
        int size = gigaSpace.read(query).getContainer().getItems().size();
        assertEquals(msg, expectedSize, size);
    }
    
    @Override
    @Test
    @Ignore
    public void testPutNull() throws InterruptedException {
        super.testPutNull();
    }
    
    @Override
    @Test
    @Ignore
    public void testPut() {
        super.testPut();
    }

    @Override
    @Test
    @Ignore
    public void testTake() {
        super.testTake();
    }
    
    @Override
    @Test
    @Ignore
    public void testOfferNullWithTimeout() throws InterruptedException {
        super.testOfferNullWithTimeout();
    }

    @Override
    @Test
    @Ignore
    public void testPollWithTimeoutEmptyQueue() throws InterruptedException {
        super.testPollWithTimeoutEmptyQueue();
    }

    @Override
    @Test
    @Ignore
    public void testPollWithTimeoutEmptyQueue2() throws InterruptedException {
        super.testPollWithTimeoutEmptyQueue2();
    }

    @Override
    @Test
    @Ignore
    public void testPollWithTimeout() throws InterruptedException {
        super.testPollWithTimeout();
    }

    @Override
    @Test
    @Ignore
    public void testOfferWithTimeout() {
        super.testOfferWithTimeout();
    }
}