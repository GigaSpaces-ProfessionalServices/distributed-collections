/**
 *
 */
package org.openspaces.collections.queue;

import com.j_spaces.core.client.SQLQuery;
import org.junit.Assume;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.openspaces.collections.queue.data.EmbeddedQueue;
import org.openspaces.collections.set.SerializableType;
import org.springframework.test.context.ContextConfiguration;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.*;
import static org.openspaces.collections.CollectionUtils.*;

/**
 * @author Svitlana_Pogrebna
 */
@RunWith(Parameterized.class)
@ContextConfiguration(locations = "classpath:/gigaqueue-embedded-test-context.xml")
@Ignore
public class EmbeddedQueueTest extends AbstractQueueTest<SerializableType> {

    public EmbeddedQueueTest(List<SerializableType> elements) {
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
        int size = gigaSpace.read(query).getContainer().getQueue().size();
        assertEquals(msg, expectedSize, size);
    }

    @Override
    @Test
    @Ignore
    public void testOfferNull() {
        super.testOfferNull();
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
    public void testOfferNullWithTimeout() throws InterruptedException {
        super.testOfferNullWithTimeout();
    }

    @Override
    @Test
    @Ignore
    public void testRemoveEmptyQueue() {
        super.testRemoveEmptyQueue();
    }

    @Override
    @Test
    @Ignore
    public void testRemoveHead() {
        super.testRemoveHead();
    }

    @Override
    @Test
    @Ignore
    public void testPoll() {
        super.testPoll();
    }

    @Override
    @Test
    @Ignore
    public void testAdd() {
        super.testAdd();
    }

    @Override
    @Test
    @Ignore
    public void testAddAll() {
        super.testAddAll();
    }

    @Override
    @Test
    @Ignore
    public void testOffer() {
        super.testOffer();
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
    public void testRemainingCapacity() {
        assertEquals("Invalid remaining capacity", Integer.MAX_VALUE, gigaQueue.remainingCapacity());
    }

    @Override
    @Test
    @Ignore
    public void testDrainToSameCollection() {
        super.testDrainToSameCollection();
    }

    @Override
    @Test
    @Ignore
    public void testDrainTo() {
        super.testDrainTo();
    }

    @Override
    @Test
    @Ignore
    public void testDrainToNullCollection() {
        super.testDrainToNullCollection();
    }

    @Override
    @Test
    @Ignore
    public void testDrainToMaxElementsNullCollection() {
        super.testDrainToMaxElementsNullCollection();
    }

    @Override
    @Test
    @Ignore
    public void testDrainToMaxElementsSameCollection() {
        super.testDrainToMaxElementsSameCollection();
    }

    @Override
    @Test
    @Ignore
    public void testDrainToMaxElements() {
        super.testDrainToMaxElements();
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
    public void testPollWithTimeout() throws InterruptedException {
        super.testPollWithTimeout();
    }

    @Override
    @Test
    @Ignore
    public void testOfferWithTimeout() {
        super.testOfferWithTimeout();
    }

    @Override
    @Test
    @Ignore
    public void testAddAllNull() {
        super.testAddAllNull();
    }

    @Override
    @Test
    @Ignore
    public void testRemoveAllNull() {
        super.testRemoveAllNull();
    }

    @Override
    @Test
    @Ignore
    public void testContainsAllNull() {
        super.testContainsAllNull();
    }

    @Override
    @Test
    @Ignore
    public void testRetainAllNull() {
        super.testRetainAllNull();
    }

    @Override
    @Test
    @Ignore
    public void testAddAllWithNull() throws InterruptedException {
        super.testAddAllWithNull();
    }

    @Override
    @Test
    @Ignore
    public void testRemoveAllWithNull() throws InterruptedException {
        super.testRemoveAllWithNull();
    }

    @Override
    @Test
    @Ignore
    public void testClear() {
        super.testClear();
    }

    @Override
    @Test
    @Ignore
    public void testContains() {
        super.testContains();
    }

    @Override
    @Test
    @Ignore
    public void testContainsAll() {
        super.testContainsAll();
    }

    @Override
    @Test
    public void testIsEmpty() {
        Assume.assumeTrue(testedElements.isEmpty());
        assertSize("Invalid queue size", 0);
    }

    @Override
    @Test
    @Ignore
    public void testIterator() {
        super.testIterator();
    }

    @Override
    @Test
    @Ignore
    public void testRemove() {
        super.testRemove();
    }

    @Override
    @Test
    @Ignore
    public void testRemoveAll() {
        super.testRemoveAll();
    }

    @Override
    @Test
    @Ignore
    public void testRemoveAllElements() {
        super.testRemoveAllElements();
    }

    @Override
    @Test
    @Ignore
    public void testRetainAll() {
        super.testRetainAll();
    }

    @Override
    @Test
    @Ignore
    public void testRetainAllEmptyCollection() {
        super.testRetainAllEmptyCollection();
    }

    @Override
    @Test
    public void testSize() {
        assertSize("Invalid queue size", testedElements.size());
    }

    @Override
    @Test
    @Ignore
    public void testToArray() {
        super.testToArray();
    }

    @Override
    @Test
    @Ignore
    public void testOfferAfterClose() throws Exception {
        super.testOfferAfterClose();
    }
}
