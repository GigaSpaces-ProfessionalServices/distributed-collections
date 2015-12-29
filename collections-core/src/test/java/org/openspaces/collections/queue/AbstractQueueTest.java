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
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.TimeUnit;

import org.junit.Assume;
import org.junit.Test;

import org.openspaces.collections.AbstractCollectionTest;

public abstract class AbstractQueueTest<T> extends AbstractCollectionTest<T> {

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
    
    // java.util.Queue methods
    @Test(expected = NoSuchElementException.class)
    public void testElementEmptyQueue() {
        Assume.assumeTrue(testedElements.isEmpty());
     
        gigaQueue.element();
    }
    
    @Test
    public void testElement() {
        Assume.assumeFalse(testedElements.isEmpty());
        
        testRetrieveHead(new RetrieveOperation<T>() {
            
            @Override
            public T perform() {
               return gigaQueue.element();
            }
        });
    }
    
    @Test(expected = NoSuchElementException.class)
    public void testElementNullValue() {
        Assume.assumeTrue(testedElements.isEmpty());
        gigaQueue.add(null);
        gigaQueue.element();
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

        checkRetrieveNullHead(operation, false);
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
        Assume.assumeFalse(testedElements.isEmpty());
        
        testRemoveInternal(new RetrieveOperation<T>() {
            @Override
            public T perform() {
                return gigaQueue.remove();
            }
        });
    }
    
    @Test(expected = NoSuchElementException.class)
    public void testRemoveNullHead() {
        Assume.assumeTrue(testedElements.isEmpty());
        gigaQueue.add(null);
        gigaQueue.remove();
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
    
    private void checkRetrieveNullHead(RetrieveOperation<T> operation, boolean remove) {
        gigaQueue.clear();
        gigaQueue.add(null);
        assertHead(remove ? 0 : 1, null, operation.perform());
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
        T element = newNotNullElement();
        T head = testedElements.isEmpty() ? element : testedElements.iterator().next();
        int size = testedElements.size();
        assertTrue("The element should be added", addOperation.perform(element));
        assertHead(++size, head, gigaQueue.peek());
        
        // an existing element
        element = testedElements.isEmpty() ? element : testedElements.iterator().next();        
        assertTrue("The element should be added", addOperation.perform(element));
        assertHead(++size, head, gigaQueue.peek());
        
        // a null element
        assertTrue("The element should be added", addOperation.perform(null));
    }
    
    @Test
    public void testAddAll() {
        assertFalse("Blocking queue should not be changed", gigaQueue.addAll(Collections.<T>emptySet()));
        int size = testedElements.size();
        assertSize("Invalid blocking queue size", testedElements.size());
        
        Collection<T> elementsToAdd = testedElements.isEmpty() ? Arrays.asList(newNotNullElement()) : testedElements;
        assertTrue("Blocking queue should be changed", gigaQueue.addAll(elementsToAdd));
        size += elementsToAdd.size();
        assertSize("Invalid blocking queue size", size);
        
        elementsToAdd = Arrays.asList(newNotNullElement(), newNotNullElement());
        assertTrue("Blocking queue should be changed", gigaQueue.addAll(elementsToAdd));
        size += elementsToAdd.size();
        assertSize("Invalid blocking queue size", size);
    }
    
    @Test
    public void testContainsNullElement() {
       assertTrue(gigaQueue.add(null));
       
       assertTrue("Blocking queue 'contains' operation result should be true", gigaQueue.contains(null));
       assertTrue("Blocking queue 'containsAll' operation result should be true", gigaQueue.containsAll(Collections.singleton(null)));
       
       Collection<T> elementsToAdd = Arrays.asList(null, newNotNullElement());
       assertTrue(gigaQueue.addAll(elementsToAdd));
       assertTrue("Blocking queue should contain elements", gigaQueue.containsAll(elementsToAdd));
    }
    
    @Test
    public void testRemoveNullElement() {
       assertTrue(gigaQueue.add(null));
       int size = testedElements.size();
       
       assertTrue("Blocking queue 'remove(E e)' operation result should be true", gigaQueue.remove(null));
       assertSize("Invalid blocking queue size", size);
       
       assertTrue(gigaQueue.add(null));
       assertTrue("Blocking queue 'removeAll' operation result should be true", gigaQueue.removeAll(Collections.singleton(null)));
       assertSize("Invalid blocking queue size", size);
       
       Collection<T> elementsToAdd = Arrays.asList(null, newNotNullElement());
       assertTrue(gigaQueue.addAll(elementsToAdd));
       assertTrue("Blocking queue should contain elements", gigaQueue.removeAll(elementsToAdd));
       assertSize("Invalid blocking queue size", size);
    }
    
    @Test
    public void testRetainAllNullElement() {
        assertTrue(gigaQueue.add(null));
       
        if (testedElements.isEmpty()) {
            assertFalse("Blocking queue 'retainAll' operation result should be true", gigaQueue.retainAll(Collections.singleton(null)));
        } else {
            assertTrue("Blocking queue 'retainAll' operation result should be true", gigaQueue.retainAll(Collections.singleton(null)));
        }
        assertSize("Invalid blocking queue size", 1);
        
        Collection<T> elementsToAdd = Arrays.asList(null, newNotNullElement());
        assertTrue(gigaQueue.addAll(elementsToAdd));
        assertTrue("Blocking queue 'retainAll' operation result should be true", gigaQueue.retainAll(Collections.singleton(null)));
        assertSize("Invalid blocking queue size", 2);
        
        assertTrue(gigaQueue.addAll(elementsToAdd));
        assertFalse("Blocking queue 'retainAll' operation result should be false", gigaQueue.retainAll(elementsToAdd));
        assertSize("Invalid blocking queue size", 4);
        
        assertTrue("Blocking queue 'retainAll' operation result should be true", gigaQueue.retainAll(Collections.singleton(newNotNullElement())));
        assertSize("Invalid blocking queue size", 0);
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
    
    @Test
    public void testTake() {
        Assume.assumeFalse(testedElements.isEmpty());

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
       
        checkRetrieveNullHead(operation, true);
    }
    
    @Test
    public void testRemainingCapacity() {
        assertEquals("Invalid remaining capacity", Integer.MAX_VALUE, gigaQueue.remainingCapacity());
        
        T element = newNotNullElement();
        assertTrue(gigaQueue.add(element));
        assertEquals("Invalid remaining capacity", Integer.MAX_VALUE, gigaQueue.remainingCapacity());
        
        assertTrue(gigaQueue.remove(element));
        assertEquals("Invalid remaining capacity", Integer.MAX_VALUE, gigaQueue.remainingCapacity());
        
        gigaQueue.clear();
        assertEquals("Invalid remaining capacity", Integer.MAX_VALUE, gigaQueue.remainingCapacity());

        List<T> elements = Arrays.asList(newNotNullElement(), null, newNotNullElement());
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

    @Test
    public void testDrainToWithNullElements() {
        int size = testedElements.size();
        gigaQueue.add(null);
        List<T> result = new ArrayList<>();
        assertEquals("Invalid number of elements transferred after performing 'drainTo' operation", ++size, gigaQueue.drainTo(result));
        verifyNullElementCanBeTransferred(size, result);
    }
    
    private void verifyNullElementCanBeTransferred(int size, Collection<T> result) {
        assertEquals("Invalid result collection size", size, result.size());
        assertSize("Blocking queue should be empty", 0);
        assertTrue("Result should contain null element", result.contains(null));
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
        
        List<T> elements = Arrays.asList(null, newNotNullElement(), null, newNotNullElement());
        int transferCount = elements.size();
        gigaQueue.addAll(elements);
        assertEquals("Invalid number of elements transferred after performing 'drainTo' operation", transferCount, gigaQueue.drainTo(result, transferCount));
        verifyNullElementCanBeTransferred(testedElements.size() + transferCount, result);
    }
    
    @Test(timeout = TIMEOUT)
    public void testPollWithTimeoutEmptyQueue() throws InterruptedException {
        Assume.assumeTrue(testedElements.isEmpty());

        gigaQueue.poll(TIMEOUT - TIMEOUT_ACCURACY, TimeUnit.MILLISECONDS);
    }
    
    @Test
    public void testPollWithTimeout() throws InterruptedException {
        Assume.assumeFalse(testedElements.isEmpty());

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
        
        checkRetrieveNullHead(operation, true);
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
    
    private interface AddOperation<T> {
        Boolean perform(T element);
    }
    
    private interface RetrieveOperation<T> {
        T perform();
    }
}
