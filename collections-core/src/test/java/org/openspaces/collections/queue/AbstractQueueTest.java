package org.openspaces.collections.queue;

import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.openspaces.collections.AbstractCollectionTest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;

import javax.annotation.Resource;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;
import static org.junit.Assume.assumeFalse;

public abstract class AbstractQueueTest<T> extends AbstractCollectionTest<T> {
    private static final Logger LOG = LoggerFactory.getLogger(AbstractQueueTest.class);

    @Value("${queue.name:}")
    protected String queueName;
    @Resource
    protected GigaBlockingQueue<T> gigaQueue;

    private static final long TIMEOUT = 1000; // in milliseconds
    private static final long TIMEOUT_ACCURACY = 10; // in milliseconds

    public AbstractQueueTest(List<T> elements) {
        super(elements);
    }

    @Override
    protected Collection<T> getCollection() {
        return gigaQueue;
    }

    @Override
    protected abstract void assertSize(String msg, int expectedSize);

    @Before
    public void setUp() {
        gigaQueue.clear();
        gigaQueue.addAll(testedElements);
    }

    @Test(expected = NullPointerException.class)
    public void testOfferNull() {
        gigaQueue.offer(null);
    }

    @Test(expected = NullPointerException.class)
    public void testPutNull() throws InterruptedException {
        gigaQueue.put(null);
    }

    @Test(expected = NullPointerException.class)
    public void testOfferNullWithTimeout() throws InterruptedException {
        gigaQueue.offer(null, 1, TimeUnit.SECONDS);
    }

    // java.util.Queue methods
    @Test(expected = NoSuchElementException.class)
    public void testElementEmptyQueue() {
        Assume.assumeTrue(testedElements.isEmpty());

        gigaQueue.element();
    }

    @Test
    public void testElement() {
        assumeFalse(testedElements.isEmpty());

        testRetrieveHead(new RetrieveOperation<T>() {

            @Override
            public T perform() {
                return gigaQueue.element();
            }
        });
    }

    @Test
    public void testPeek() {
        if (testedElements.isEmpty()) {
            assertNull("The retrieved element should be null in case of empty blocking queue", gigaQueue.peek());
            return;
        }

        RetrieveOperation<T> operation = new RetrieveOperation<T>() {

            @Override
            public T perform() {
                return gigaQueue.peek();
            }
        };

        testRetrieveHead(operation);
    }

    private void testRetrieveHead(RetrieveOperation<T> operation) {
        T head = operation.perform();
        assertHead(testedElements.size(), testedElements.get(0), head);

        T head1 = operation.perform();
        assertEquals("Blocking queue head should be the same", head, head1);
    }

    @Test(expected = NoSuchElementException.class)
    public void testRemoveEmptyQueue() {
        Assume.assumeTrue(testedElements.isEmpty());

        gigaQueue.remove();
    }

    @Test
    public void testRemoveHead() {
        assumeFalse(testedElements.isEmpty());

        testRemoveInternal(new RetrieveOperation<T>() {
            @Override
            public T perform() {
                return gigaQueue.remove();
            }
        });
    }

    private void testRemoveInternal(RetrieveOperation<T> operation) {
        T head = operation.perform();
        int size = testedElements.size();
        assertHead(--size, testedElements.get(0), head);

        Assume.assumeTrue(testedElements.size() > 1);
        T head1 = operation.perform();
        assertNotEquals("Blocking queue head should not be the same", head, head1);
        assertHead(--size, testedElements.get(1), head1);
    }

    @Test
    public void testPoll() {
        if (testedElements.isEmpty()) {
            assertNull("The retrieved element should be null in case of empty blocking queue", gigaQueue.poll());
            return;
        }
        testRemoveInternal(new RetrieveOperation<T>() {
            @Override
            public T perform() {
                return gigaQueue.poll();
            }
        });
    }

    private void assertHead(int expectedSize, T expected, T actual) {
        assertEquals("Invalid blocking queue head", expected, actual);
        assertSize("Invalid blocking queue size", expectedSize);
    }

    // java.util.BlockingQueue methods
    @Test
    public void testAdd() {
        testAddInternal(new AddOperation<T>() {

            @Override
            public Boolean perform(T element) {
                return gigaQueue.add(element);
            }
        });
    }

    private void testAddInternal(AddOperation<T> addOperation) {
        // a new element
        T element = newElement();
        T head = testedElements.isEmpty() ? element : testedElements.iterator().next();
        int size = testedElements.size();
        assertTrue("The element should be added", addOperation.perform(element));
        assertHead(++size, head, gigaQueue.peek());

        // an existing element
        element = testedElements.isEmpty() ? element : testedElements.iterator().next();
        assertTrue("The element should be added", addOperation.perform(element));
        assertHead(++size, head, gigaQueue.peek());
    }

    @Test
    public void testAddAll() {
        assertFalse("Blocking queue should not be changed", gigaQueue.addAll(Collections.<T>emptySet()));
        int size = testedElements.size();
        assertSize("Invalid blocking queue size", testedElements.size());

        Collection<T> elementsToAdd = testedElements.isEmpty() ? Arrays.asList(newElement()) : testedElements;
        assertTrue("Blocking queue should be changed", gigaQueue.addAll(elementsToAdd));
        size += elementsToAdd.size();
        assertSize("Invalid blocking queue size", size);

        elementsToAdd = Arrays.asList(newElement(), newElement());
        assertTrue("Blocking queue should be changed", gigaQueue.addAll(elementsToAdd));
        size += elementsToAdd.size();
        assertSize("Invalid blocking queue size", size);
    }

    @Test
    public void testOffer() {
        testAddInternal(new AddOperation<T>() {

            @Override
            public Boolean perform(T element) {
                return gigaQueue.offer(element);
            }
        });
    }

    @Test
    public void testPut() {
        testAddInternal(new AddOperation<T>() {

            @Override
            public Boolean perform(T element) {
                try {
                    gigaQueue.put(element);
                    return true;
                } catch (InterruptedException e) {
                    fail(e.getMessage());
                }
                return null;
            }
        });
    }

    @Test(timeout = 5000)
    public void testTake() {
        assumeFalse(testedElements.isEmpty());

        RetrieveOperation<T> operation = new RetrieveOperation<T>() {

            @Override
            public T perform() {
                try {
                    return gigaQueue.take();
                } catch (InterruptedException e) {
                    fail(e.getMessage());
                }
                return null;
            }
        };

        testRemoveInternal(operation);
    }

    @Test
    public void testRemainingCapacity() {
        assertEquals("Invalid remaining capacity", Integer.MAX_VALUE, gigaQueue.remainingCapacity());

        T element = newElement();
        assertTrue(gigaQueue.add(element));
        assertEquals("Invalid remaining capacity", Integer.MAX_VALUE, gigaQueue.remainingCapacity());

        assertTrue(gigaQueue.remove(element));
        assertEquals("Invalid remaining capacity", Integer.MAX_VALUE, gigaQueue.remainingCapacity());

        gigaQueue.clear();
        assertEquals("Invalid remaining capacity", Integer.MAX_VALUE, gigaQueue.remainingCapacity());

        List<T> elements = Arrays.asList(newElement(), newElement());
        assertTrue(gigaQueue.addAll(elements));
        assertEquals("Invalid remaining capacity", Integer.MAX_VALUE, gigaQueue.remainingCapacity());

        assertTrue(gigaQueue.removeAll(elements));
        assertEquals("Invalid remaining capacity", Integer.MAX_VALUE, gigaQueue.remainingCapacity());

    }

    @Test(expected = IllegalArgumentException.class)
    public void testDrainToSameCollection() {
        gigaQueue.drainTo(gigaQueue);
    }

    @Test
    public void testDrainTo() {
        Collection<T> result = new ArrayList<>();
        assertEquals("Invalid number of elements transferred after performing 'drainTo' operation", testedElements.size(), gigaQueue.drainTo(result));

        verifyAllElementsTransferred(result);
    }

    private void verifyAllElementsTransferred(Collection<T> result) {
        assertSize("Blocking queue should be empty", 0);
        assertEquals("Invalid result collection size", testedElements.size(), result.size());
        for (T actual : result) {
            assertNotNull("Element should not be null", actual);
            assertTrue("Invalid element", testedElements.contains(actual));
        }
    }

    @Test(expected = NullPointerException.class)
    public void testDrainToNullCollection() {
        gigaQueue.drainTo(null);
    }

    @Test(expected = NullPointerException.class)
    public void testDrainToMaxElementsNullCollection() {
        gigaQueue.drainTo(null, Integer.MAX_VALUE);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testDrainToMaxElementsSameCollection() {
        gigaQueue.drainTo(gigaQueue, Integer.MAX_VALUE);
    }

    @Test
    public void testDrainToMaxElements() {
        Collection<T> result = new ArrayList<>();
        int size = testedElements.size();
        assertEquals("No elements should be transferred due to negative max elements param", 0, gigaQueue.drainTo(result, Integer.MIN_VALUE));
        assertSize("Blocking queue size should not be changed", size);

        if (testedElements.isEmpty()) {
            assertEquals("Invalid number of elements transferred after performing 'drainTo' operation", 0, gigaQueue.drainTo(result, 1));
            assertSize("Blocking queue should be empty", 0);
            assertEquals("Invalid result collection size", 0, result.size());
        } else {
            assertEquals("Invalid number of elements transferred after performing 'drainTo' operation", 1, gigaQueue.drainTo(result, 1));
            assertSize("Blocking queue size should not be changed", --size);

            assertEquals("Invalid result collection size", 1, result.size());

            T element = result.iterator().next();
            assertNotNull("Element should not be null", element);
            //relies on the elements order preserving
            assertEquals("Invalid element transferred", testedElements.iterator().next(), element);
        }

        Assume.assumeTrue(testedElements.size() > 1);
        assertEquals("Invalid number of elements transferred after performing 'drainTo' operation", size, gigaQueue.drainTo(result, Integer.MAX_VALUE));
        verifyAllElementsTransferred(result);
    }

    @Test(timeout = TIMEOUT)
    public void testPollWithTimeoutEmptyQueue() throws InterruptedException {
        Assume.assumeTrue(testedElements.isEmpty());

        gigaQueue.poll(TIMEOUT - TIMEOUT_ACCURACY, TimeUnit.MILLISECONDS);
    }

    @Test
    public void testPollWithTimeout() throws InterruptedException {
        assumeFalse(testedElements.isEmpty());

        RetrieveOperation<T> operation = new RetrieveOperation<T>() {
            @Override
            public T perform() {
                try {
                    return gigaQueue.poll(TIMEOUT, TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                    fail(e.getMessage());
                }
                return null;
            }
        };

        testRemoveInternal(operation);
    }

    @Test
    public void testOfferWithTimeout() {
        testAddInternal(new AddOperation<T>() {

            @Override
            public Boolean perform(T element) {
                try {
                    return gigaQueue.offer(element, TIMEOUT, TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                    fail(e.getMessage());
                }
                return null;
            }
        });
    }

    // iterator.remove() tests
    @Test
    public void testCallingRemoveOnIteratorConcurrently() {
        Assume.assumeTrue(testedElements.size() >= 2);

        // creating iterator before the polling
        Iterator<T> iterator = gigaQueue.iterator();

        // polling half of the elements
        for (int index = 0; index < testedElements.size() / 2; index++) {
            gigaQueue.poll();
        }
        int expectedSize = testedElements.size() - testedElements.size() / 2;

        // removing first item from iterator - that should be polled in loop above
        assertTrue(iterator.hasNext());
        iterator.next();
        iterator.remove();

        // checking queue size - should not be affected by iterator
        assertEquals(expectedSize, gigaQueue.size());
    }

    @Test
    public void testCallingRemoveOnLastElement() {
        assumeFalse(testedElements.isEmpty());

        T element = newElement();
        gigaQueue.add(element);
        // calls iterator.remove() inside
        gigaQueue.remove(element);

        assertEquals(testedElements.size(), gigaQueue.size());

        gigaQueue.clear();
        assertEquals(0, gigaQueue.size());
    }

    @Test
    public void testRemovingWholeQueueWithIterator() {
        Iterator<T> iterator = gigaQueue.iterator();
        while (iterator.hasNext()) {
            assertNotNull(iterator.next());
            iterator.remove();
        }
        assertSize("Queue must be empty", 0);
    }

    @Test
    public void testRemovingMiddleElementWithIterator() {
        assumeFalse(testedElements.isEmpty());

        int middleIndex = testedElements.size() / 2 - 1;
        Iterator<T> iterator = gigaQueue.iterator();
        for (int index = 0; index <= middleIndex; index++) {
            assertTrue(iterator.hasNext());
            assertNotNull(iterator.next());
        }
        iterator.remove();

        assertSize("Queue must be missing one element", testedElements.size() - 1);

        for (int index = 0; index < testedElements.size(); index++) {
            // skip middle element
            if (index != middleIndex) {
                T expected = testedElements.get(index);
                assertEquals(expected, gigaQueue.poll());
            }
        }
    }

    @Test(expected = IllegalStateException.class)
    public void testIteratorRemoveWithoutNext() {
        gigaQueue.iterator().remove();
    }

    private interface AddOperation<T> {
        Boolean perform(T element);
    }

    private interface RetrieveOperation<T> {
        T perform();
    }
}
