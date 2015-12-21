package org.openspaces.core.collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Set;

import javax.annotation.Resource;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.openspaces.core.GigaSpace;
import org.springframework.test.context.TestContextManager;

public abstract class AbstractCollectionTest<T> {

    @Resource
    protected GigaSpace gigaSpace;
    
    protected Collection<T> testedElements;
    
    protected abstract Collection<T> getCollection();
    
    protected AbstractCollectionTest(Collection<T> elements) {
        this.testedElements = elements;
        setUpSpringContext();
    }
    
    @Before
    public void setUp() {
        getCollection().addAll(testedElements);
    }
    
    @After
    public void clear() {
        gigaSpace.clear(null);
    }
    
    private void setUpSpringContext() {
        try {
            new TestContextManager(getClass()).prepareTestInstance(this);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to set spring context up", e);
        }
    }
    
    @Test(expected = NullPointerException.class)
    public void testAddNull() {
        getCollection().add(null);
    }
    
    @Test
    public void testAdd() {
        final T newElement = newNotNullElement();
        final Collection<T> collection = getCollection();
        assertTrue("The result of adding a new element: " + newElement + " should be true", collection.add(newElement));
        int expectedSize = testedElements.size() + 1;
        assertEquals("Invalid collection size", expectedSize, gigaSpace.count(null));
        
        T existingElement = testedElements.isEmpty() ? newElement : testedElements.iterator().next();
        assertFalse("The result of adding an existing element: " + existingElement + " should be false", collection.add(existingElement));
        assertEquals("Invalid collection size", expectedSize, gigaSpace.count(null));
    }
    
    @Test(expected = NullPointerException.class)
    public void testAddAllNull() {
        getCollection().addAll(null);
    }
    
    @Test(expected = NullPointerException.class)
    public void testAddAllNullElement() {
        getCollection().addAll(Arrays.asList(null));
    }
    
    @Test
    public void testAddAll() {
        final Collection<T> collection = getCollection();
        assertFalse("Collection should not be changed after adding empty set", collection.addAll(Collections.emptySet()));
        int expectedSize = testedElements.size();
        assertEquals("Invalid collection size", expectedSize, gigaSpace.count(null));
        
        assertFalse("Collection should not be changed after adding empty list", collection.addAll(Collections.emptyList()));
        assertEquals("Invalid collection size", expectedSize, gigaSpace.count(null));
        
        assertFalse("Collection should not be changed after adding all its elements", collection.addAll(testedElements));
        assertEquals("Invalid collection size", expectedSize, gigaSpace.count(null));
        
        assertFalse("Collection should not be changed after adding a sub collection of its elements", collection.addAll(getTestedDataSubCollection()));
        assertEquals("Invalid collection size", expectedSize, gigaSpace.count(null));
        
        List<T> newElementsList = Collections.singletonList(newNotNullElement());
        assertTrue("Collection should be changed after new element from list = " + newElementsList, collection.addAll(newElementsList));
        assertEquals("Invalid collection size", ++expectedSize, gigaSpace.count(null));
        
        Queue<T> newElementsQueue = new LinkedList<>(newElementsList);
        newElementsQueue.offer(newNotNullElement());
        newElementsQueue.offer(newNotNullElement());
        newElementsQueue.offer(newNotNullElement());
        expectedSize += 3;
        assertTrue("Collection should be changed after adding new elements from queue = " + newElementsQueue, collection.addAll(newElementsQueue));
        assertEquals("Invalid collection size", expectedSize, gigaSpace.count(null));
        
        Set<T> newElementsSet = new HashSet<>(testedElements);
        newElementsSet.add(newNotNullElement());
        assertTrue("Collection should be changed after adding existing and new elements from set = " + newElementsSet, collection.addAll(newElementsSet));
        assertEquals("Invalid collection size", ++expectedSize, gigaSpace.count(null));
    }
    
    @Test
    public void testClear() {
        getCollection().clear();
        
        assertEquals("Invalid collection size", 0, gigaSpace.count(null));
    }
    
    @Test(expected = NullPointerException.class)
    public void testContainsNull() {
        getCollection().contains(null);
    }
    
    @Test
    public void testContains() {
        final Collection<T> collection = getCollection();
        if (!testedElements.isEmpty()) {
            T element = testedElements.iterator().next();
            assertTrue("Collection should contain the element = " + element, collection.contains(element));
        }
        
        T element = newNotNullElement();
        assertFalse("Collection should not contain the element = " + element, collection.contains(element));

      /*  Object newType = new Serializable() {};
        assertFalse("Collection should not contain the element = " + newType, gigaSet.contains(newType));*/
    }
    
    @Test(expected = NullPointerException.class)
    public void testContainsAllNull() {
        getCollection().containsAll(null);
    }
    
    @Test(expected = NullPointerException.class)
    public void testContainsAllNullElement() {
        getCollection().containsAll(Arrays.asList(null));
    }
    
    @Test
    public void testContainsAll() {
        final Collection<T> collection = getCollection();
        assertTrue("Collection should contain empty set", collection.containsAll(Collections.emptySet()));
        assertTrue("Collection should contain empty list", collection.containsAll(Collections.emptyList()));
        
        assertTrue("Collection should contain all its elements", collection.containsAll(testedElements));
        assertTrue("Collection should contain all its elements", collection.containsAll(new ArrayList<>(testedElements)));
        
        Collection<T> subCollection = getTestedDataSubCollection();
        assertTrue("Collection should contain subset of its elements", collection.containsAll(subCollection));
        assertTrue("Collection should contain subset of its elements", collection.containsAll(new ArrayList<>(subCollection)));
        
        Set<T> newElementsSet = Collections.singleton(newNotNullElement());
        assertFalse("Collection should not contain the elements from the collection = " + newElementsSet, collection.containsAll(newElementsSet));
        
        List<T> newElementsList = Collections.singletonList(newNotNullElement());
        assertFalse("Collection should not contain the elements from the collection = " + newElementsList, collection.containsAll(newElementsList));

        Set<T> elementsSet = new HashSet<>(testedElements);
        elementsSet.add(newNotNullElement());
        assertFalse("Collection should not contain the elements from the collection = " + elementsSet, collection.containsAll(elementsSet));
        
        Queue<T> elementsQueue = new LinkedList<>(testedElements);
        elementsQueue.offer(newNotNullElement());
        assertFalse("Collection should not contain the elements from the collection = " + elementsQueue, collection.containsAll(elementsQueue));
        
      /*  List<?> newTypeElements = Arrays.asList(new Serializable(){});
        assertFalse("Collection should not contain the elements from the collection = " + newTypeElements, gigaSet.containsAll(newTypeElements));*/
    }
    
    @Test
    public void testIsEmpty() {
        final Collection<T> collection = getCollection();
        if (!testedElements.isEmpty()) {
            assertFalse("Collection should not be empty", collection.isEmpty());
        } else {
            assertTrue("Collection should be empty", collection.isEmpty());
        }
        
        T element = newNotNullElement();
        collection.add(element);
        assertFalse("Collection should not be empty", collection.isEmpty());
        
        collection.remove(element);
        if (!testedElements.isEmpty()) {
            assertFalse("Collection should not be empty", collection.isEmpty());
        } else {
            assertTrue("Collection should be empty", collection.isEmpty());
        }
        
        collection.clear();
        assertTrue("Collection should be empty", collection.isEmpty());
    }
    
    @Test
    public void testIterator() {
        Iterator<T> iterator = getCollection().iterator();
        assertNotNull("Iterator must not be null", iterator);
        int actualCount = 0;
        while(iterator.hasNext()) {
            assertNotNull("Collection elements must not be null", iterator.next());
            actualCount++;
        }
        assertEquals("Invalid collection elements count", actualCount, gigaSpace.count(null));
    }
    
    @Test(expected = NullPointerException.class)
    public void testRemoveNull() {
        getCollection().remove(null);
    }
    
    @Test
    public void testRemove() {
        int expectedSize = testedElements.size();
        final Collection<T> collection = getCollection();
        if (!testedElements.isEmpty()) {
            T element = testedElements.iterator().next();
            assertTrue("Collection should return true after removing an existing element = " + element, collection.remove(element));
            assertEquals("Invalid collection size", --expectedSize, gigaSpace.count(null));
        }
        
        T newElement = newNotNullElement();
        assertFalse("Collection should return true after removing an unexisting element = " + newElement, collection.remove(newElement));
        assertEquals("Invalid collection size", expectedSize, gigaSpace.count(null));
        
       /* Object newType = new Serializable() {};
        assertFalse("GigaSet should return true after removing an unexisting element = " + newType, gigaSet.remove(newType));
        assertEquals("Invalid gigaSet size", expectedSize, gigaSpace.count(null));*/
    }
    
    @Test(expected = NullPointerException.class)
    public void testRemoveAllNull() {
        getCollection().removeAll(null);
    }
    
    @Test(expected = NullPointerException.class)
    public void testRemoveAllNullElement() {
        getCollection().removeAll(Arrays.asList(null));
    }
    
    @Test
    public void testRemoveAll() {
        final Collection<T> collection = getCollection();
        assertFalse("Collection should not be changed after removing empty set", collection.removeAll(Collections.emptySet()));
        int expectedSize = testedElements.size();
        assertEquals("Invalid collection size", expectedSize, gigaSpace.count(null));
        
        List<T> newElementsList = Collections.singletonList(newNotNullElement());
        assertFalse("Collection should not be changed after removing list with unexisting elements", collection.removeAll(newElementsList));
        
        Collection<T> subCollection = getTestedDataSubCollection();
        expectedSize -= subCollection.size();
        subCollection.add(newNotNullElement());
        subCollection.add(newNotNullElement());
        
        if (!testedElements.isEmpty()) {
            assertTrue("Collection should be changed after removing a collection with existing and unexisting elements", collection.removeAll(subCollection));
        } else {
            assertFalse("Collection should not be changed after removing some elements from empty collection", collection.removeAll(subCollection));
        }
        
        assertEquals("Invalid collection size", expectedSize, gigaSpace.count(null));
    }
    
    @Test
    public void testRemoveAllElements() {
        if (!testedElements.isEmpty()) {
            assertTrue("Collection should be changed after removing all its elements", getCollection().removeAll(testedElements));
            assertEquals("Invalid collection size", 0, gigaSpace.count(null));
        }
    }
    
    @Test(expected = NullPointerException.class)
    public void testRetainAllNull() {
        getCollection().retainAll(null);
    }
    
    @Test(expected = NullPointerException.class)
    public void testRetainAllNullElement() {
        getCollection().retainAll(Arrays.asList(null));
    }
    
    @Test
    public void testRetainAll() {
        final Collection<T> collection = getCollection();
        assertFalse("Collection should not be changed after retaining all its elements", collection.retainAll(testedElements));
        assertEquals("Invalid collection size", testedElements.size(), gigaSpace.count(null));
        
        Collection<T> subCollection = getTestedDataSubCollection();
        if (subCollection.isEmpty() || testedElements.size() == subCollection.size()) {
            assertFalse("Collection should not be changed after retaining all its elements", collection.retainAll(subCollection));
        } else {
            assertTrue("Collection should be changed after retaining a sub collection of its elements", collection.retainAll(subCollection));
        }
        assertEquals("Invalid collection size", subCollection.size(), gigaSpace.count(null));
    }
    
    @Test
    public void testRetainAllEmptyCollection() {
        if (!testedElements.isEmpty()) {
            assertTrue("Collection should be changed after retaining an empty collection", getCollection().retainAll(Collections.emptyList()));
            assertEquals("Invalid collection size", 0, gigaSpace.count(null));
        }
    }
    
    @Test
    public void testSize() {
        int expectedSize = testedElements.size();
        final Collection<T> collection = getCollection();
        assertEquals("Invalid collection size", expectedSize, collection.size());
        
        T element = newNotNullElement();
        collection.add(element);
        assertEquals("Invalid collection size", ++expectedSize, collection.size());
        
        collection.remove(element);
        assertEquals("Invalid collection size", --expectedSize, collection.size());
        
        collection.clear();
        assertEquals("Invalid collection size", 0, collection.size());
    }
   
    @Test
    public void testToArray() {
        final Collection<T> collection = getCollection();
        Object[] objectArray = collection.toArray();
        assertNotNull("Array of collection elements must not be null", objectArray);
        assertEquals("Invalid collection elements count", objectArray.length, gigaSpace.count(null));
        for (Object o : objectArray) {
            assertNotNull("Collection elements must not be null", o);
            assertTrue("Invalid element type", o.getClass().isAssignableFrom(getElementType()));
        }
        
        T[] elementArray = collection.toArray(getElementArray());
        assertNotNull("Array of collection elements must not be null", elementArray);
        assertEquals("Invalid collection elements count", elementArray.length, gigaSpace.count(null));
        for (T o : elementArray) {
            assertNotNull("Collection elements must not be null", o);
        }
    }
    
    protected abstract Class<? extends T> getElementType();
    
    protected abstract T[] getElementArray();
    
    protected Collection<T> getTestedDataSubCollection() {
        Collection<T> subSet = new HashSet<>();
        int i = 0;
        for (T e : testedElements) {
            if (i ++ % 2 == 0) {
                subSet.add(e);
            }
        }
        return subSet;
    }
    
    protected abstract T newNotNullElement();
}
