package org.openspaces.collections.queue;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.NoSuchElementException;

import org.junit.Assume;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.openspaces.collections.AbstractCollectionTest;
import org.openspaces.collections.queue.data.QueueItem;


@RunWith(Parameterized.class)
public abstract class AbstractGigaBlockingQueueTest<T> extends AbstractCollectionTest<T> {

    protected GigaBlockingQueue<T> gigaQueue;
    
    private static final String QUEUE_NAME = "TestGigaBlockingQueue";
    
    protected int capacity;
    
    public AbstractGigaBlockingQueueTest(List<T> elements, int capacity) {
        super(elements);
        this.capacity = capacity;
        this.gigaQueue = new DefaultGigaBlockingQueue<>(gigaSpace, QUEUE_NAME, capacity);
    }

    @Override
    protected Collection<T> getCollection() {
        return gigaQueue;
    }
    
    @Override
    protected void assertSize(String msg, int expectedSize) {
        QueueItem<T> queueItem = new QueueItem<>();
        assertEquals(msg, expectedSize, gigaSpace.count(queueItem));
    }
    
    // java.util.Queue methods
    @Test(expected = NoSuchElementException.class)
    public void testElementEmptyQueue() {
        Assume.assumeTrue(testedElements.isEmpty());
     
        gigaQueue.element();
    }
    
    @Test
    public void testElement() {
        Assume.assumeFalse(testedElements.isEmpty());
        
        T head = gigaQueue.element();
        assertHead(testedElements.size(), testedElements.get(0), head);
        
        T head1 = gigaQueue.element();
        assertEquals("Blocking queue head should be the same", head, head1);
    }
    
    @Test
    public void testPeek() {
        if (testedElements.isEmpty()) {
            assertNull("The retrieved element should be null in case of empty blocking queue", gigaQueue.peek());
            return;
        } 
        
        T head = gigaQueue.peek();
        assertHead(testedElements.size(), testedElements.get(0), head);
        
        T head1 = gigaQueue.peek();
        assertEquals("Blocking queue head should be the same", head, head1);
    }
    
    @Test(expected = NoSuchElementException.class)
    public void testRemoveEmptyQueue() {
        Assume.assumeTrue(testedElements.isEmpty());
       
        gigaQueue.remove();
    }
    
    @Test
    public void testRemoveHead() {
        Assume.assumeFalse(testedElements.isEmpty());
        
        T head = gigaQueue.remove();
        int size = testedElements.size();
        assertHead(--size, testedElements.get(0), head);
        
        Assume.assumeTrue(testedElements.size() > 1);
        T head1 = gigaQueue.remove();
        assertNotEquals("Blocking queue head should not be the same", head, head1);
        assertHead(--size, testedElements.get(1), head1);
    }
    
    @Test
    public void testPoll() {
        if (testedElements.isEmpty()) {
            assertNull("The retrieved element should be null in case of empty blocking queue", gigaQueue.poll());
            return;
        } 
        T head = gigaQueue.poll();
        int size = testedElements.size();
        assertHead(--size, testedElements.get(0), head);
        
        Assume.assumeTrue(testedElements.size() > 1);
        T head1 = gigaQueue.poll();
        assertNotEquals("Blocking queue head should not be the same", head, head1);
        assertHead(--size, testedElements.get(1), head1);
    }
    
    private void assertHead(int expectedSize, T expected, T actual) {
        assertNotNull("Blocking queue head should not be null");
        assertEquals("Invalid blocking queue head", expected, actual);
        assertSize("Blocking queue size should not be changed", expectedSize);
    }
    
    // java.util.BlockingQueue methods
    @Test(expected = IllegalStateException.class)
    public void testAddToFullQueue() {
        populateQueue();
        
        gigaQueue.add(newNotNullElement());
    }
    
    @Test(expected = NullPointerException.class)
    public void testOfferNull() {
        gigaQueue.offer(null);
    }
    
    @Test
    public void testOffer() {
        // a new element
        T element = newNotNullElement();
        int size = testedElements.size();
        assertTrue("The element should be added", gigaQueue.offer(element));
        assertSize("Blocking queue size should not be changed", ++size);
        assertHead(size, element, gigaQueue.peek());
        
        //an existing element
        Assume.assumeFalse(testedElements.isEmpty());
        element = testedElements.iterator().next();
        assertTrue("The element should be added", gigaQueue.offer(element));
        assertSize("Blocking queue size should not be changed", ++size);
        assertHead(size, element, gigaQueue.peek());
    }
    
    @Test
    public void testOfferToFullQueue() {
        populateQueue();
        
        assertFalse("The element should not be inserted due to reaching the capacity", gigaQueue.offer(newNotNullElement()));
    }
    
    @Test
    public void testRemainingCapacity() {
        int expectedCapacity = testedElements.size() - capacity;
        assertEquals("Invalid remaining capacity", expectedCapacity, gigaQueue.remainingCapacity());
        
        T element = newNotNullElement();
        assertTrue(gigaQueue.add(element));
        assertEquals("Invalid remaining capacity", --expectedCapacity, gigaQueue.remainingCapacity());
        
        List<T> elements = Arrays.asList(newNotNullElement(), newNotNullElement());
        assertTrue(gigaQueue.addAll(elements));
        expectedCapacity -= elements.size();
        assertEquals("Invalid remaining capacity", expectedCapacity, gigaQueue.remainingCapacity());
        
        assertTrue(gigaQueue.remove(element));
        assertEquals("Invalid remaining capacity", ++expectedCapacity, gigaQueue.remainingCapacity());
        
        assertTrue(gigaQueue.removeAll(elements));
        expectedCapacity += elements.size();
        assertEquals("Invalid remaining capacity", expectedCapacity, gigaQueue.remainingCapacity());
        
        gigaQueue.clear();
        assertEquals("Invalid remaining capacity", capacity, gigaQueue.remainingCapacity());
    }
    
    @Test(expected = NullPointerException.class)
    public void testDrainToNullCollection() {
        gigaQueue.drainTo(null);
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
        //relies on the elements order preserving
        for (T expected : testedElements) {
            for (T actual : result) {
                assertNotNull("Element should not be null", actual);
                assertEquals("Invalid element", expected, actual);
            }
        }
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
        gigaQueue.drainTo(result, Integer.MAX_VALUE);
        verifyAllElementsTransferred(result);
    }
    
    private void populateQueue() {
        int size = testedElements.size();
        for (int i = 0 ; i < size - capacity; i++) {
            try {
                T element = newNotNullElement();
                assertTrue("The element should be added", gigaQueue.add(element));
                assertSize("Blocking queue size should not be changed", ++size);
                assertHead(size, element, gigaQueue.peek());
            } catch(IllegalStateException e) {
                fail("The capacity restriction should not have been violated");
            }
        }
    }
}
