package org.openspaces.core.collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Set;

import javax.annotation.Resource;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.openspaces.core.GigaSpace;
import org.springframework.test.context.TestContextManager;

@RunWith(Parameterized.class)
public abstract class AbstractGigaSetTest<T extends Serializable> {

    @Resource
    protected GigaSet<T> gigaSet;
    
    @Resource
    protected GigaSpace gigaSpace;
    
    protected Set<T> testElements;

    public AbstractGigaSetTest(Set<T> elements) {
        setUpSpringContext();
        this.testElements = elements;
    }
    
    private void setUpSpringContext() {
        try {
            new TestContextManager(getClass()).prepareTestInstance(this);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to set spring context up", e);
        }
    }
    
    @Before
    public void setUp() {
        gigaSet.addAll(testElements);
    }
    
    @After
    public void clear() {
        gigaSpace.clear(null);
    }
    
    @Test
    public void testSize() {
        int expectedSize = testElements.size();
        assertEquals("Invalid gigaSet size", expectedSize, gigaSet.size());
        
        T element = newNotNullElement();
        gigaSet.add(element);
        assertEquals("Invalid gigaSet size", ++expectedSize, gigaSet.size());
        
        gigaSet.remove(element);
        assertEquals("Invalid gigaSet size", --expectedSize, gigaSet.size());
        
        gigaSet.clear();
        assertEquals("Invalid gigaSet size", 0, gigaSet.size());
    }
   
    @Test
    public void testEmpty() {
        if (!testElements.isEmpty()) {
            assertFalse("GigaSet should not be empty", gigaSet.isEmpty());
        } else {
            assertTrue("GigaSet should be empty", gigaSet.isEmpty());
        }
        
        T element = newNotNullElement();
        gigaSet.add(element);
        assertFalse("GigaSet should not be empty", gigaSet.isEmpty());
        
        gigaSet.remove(element);
        if (!testElements.isEmpty()) {
            assertFalse("GigaSet should not be empty", gigaSet.isEmpty());
        } else {
            assertTrue("GigaSet should be empty", gigaSet.isEmpty());
        }
        
        gigaSet.clear();
        assertTrue("GigaSet should be empty", gigaSet.isEmpty());
    }
    
    @Test(expected = NullPointerException.class)
    public void testContainsNull() {
        gigaSet.contains(null);
    }
    
    @Test
    public void testContains() {
        if (!testElements.isEmpty()) {
            T element = testElements.iterator().next();
            assertTrue("GigaSet should contain the element = " + element, gigaSet.contains(element));
        }
        
        T element = newNotNullElement();
        assertFalse("GigaSet should not contain the element = " + element, gigaSet.contains(element));

      /*  Object newType = new Serializable() {};
        assertFalse("GigaSet should not contain the element = " + newType, gigaSet.contains(newType));*/
    }
    
    @Test(expected = NullPointerException.class)
    public void testContainsAllNull() {
        gigaSet.containsAll(null);
    }
    
    @Test(expected = NullPointerException.class)
    public void testContainsAllNullElement() {
        gigaSet.containsAll(Arrays.asList(null));
    }
    
    @Test
    public void testContainsAll() {
        assertTrue("GigaSet should contain empty set", gigaSet.containsAll(Collections.emptySet()));
        assertTrue("GigaSet should contain empty list", gigaSet.containsAll(Collections.emptyList()));
        
        assertTrue("GigaSet should contain all its elements", gigaSet.containsAll(testElements));
        assertTrue("GigaSet should contain all its elements", gigaSet.containsAll(new ArrayList<>(testElements)));
        
        Set<T> subSet = getTestDataSubSet();
        assertTrue("GigaSet should contain subset of its elements", gigaSet.containsAll(subSet));
        assertTrue("GigaSet should contain subset of its elements", gigaSet.containsAll(new ArrayList<>(subSet)));
        
        Set<T> newElementsSet = Collections.singleton(newNotNullElement());
        assertFalse("GigaSet should not contain the elements from the collection = " + newElementsSet, gigaSet.containsAll(newElementsSet));
        
        List<T> newElementsList = Collections.singletonList(newNotNullElement());
        assertFalse("GigaSet should not contain the elements from the collection = " + newElementsList, gigaSet.containsAll(newElementsList));

        Set<T> elementsSet = new HashSet<>(testElements);
        elementsSet.add(newNotNullElement());
        assertFalse("GigaSet should not contain the elements from the collection = " + elementsSet, gigaSet.containsAll(elementsSet));
        
        Queue<T> elementsQueue = new LinkedList<>(testElements);
        elementsQueue.offer(newNotNullElement());
        assertFalse("GigaSet should not contain the elements from the collection = " + elementsQueue, gigaSet.containsAll(elementsQueue));
        
      /*  List<?> newTypeElements = Arrays.asList(new Serializable(){});
        assertFalse("GigaSet should not contain the elements from the collection = " + newTypeElements, gigaSet.containsAll(newTypeElements));*/
    }
    
    @Test(expected = NullPointerException.class)
    public void testAddNull() {
        gigaSet.add(null);
    }
    
    @Test
    public void testAdd() {
        T newElement = newNotNullElement();
        assertTrue("GigaSet should return true after adding a new element " + newElement, gigaSet.add(newElement));
        int expectedSize = testElements.size() + 1;
        assertEquals("Invalid gigaSet size", expectedSize, gigaSpace.count(null));
        
        T existingElement = testElements.isEmpty() ? newElement : testElements.iterator().next();
        assertFalse("GigaSet should return false after adding an existing element " + existingElement, gigaSet.add(existingElement));
        assertEquals("Invalid gigaSet size", expectedSize, gigaSpace.count(null));
    }
    
    @Test(expected = NullPointerException.class)
    public void testAddAllNull() {
        gigaSet.addAll(null);
    }
    
    @Test(expected = NullPointerException.class)
    public void testAddAllNullElement() {
        gigaSet.addAll(Arrays.asList(null));
    }
    
    @Test
    public void testAddAll() {
        assertFalse("GigaSet should not be changed after adding empty set", gigaSet.addAll(Collections.emptySet()));
        int expectedSize = testElements.size();
        assertEquals("Invalid gigaSet size", expectedSize, gigaSpace.count(null));
        
        assertFalse("GigaSet should not be changed after adding empty set", gigaSet.addAll(Collections.emptyList()));
        assertEquals("Invalid gigaSet size", expectedSize, gigaSpace.count(null));
        
        assertFalse("GigaSet should not be changed after adding all its elements", gigaSet.addAll(testElements));
        assertEquals("Invalid gigaSet size", expectedSize, gigaSpace.count(null));
        
        assertFalse("GigaSet should not be changed after adding a subset of its elements", gigaSet.addAll(getTestDataSubSet()));
        assertEquals("Invalid gigaSet size", expectedSize, gigaSpace.count(null));
        
        List<T> newElementsList = Collections.singletonList(newNotNullElement());
        assertTrue("GigaSet should be changed after new element from list = " + newElementsList, gigaSet.addAll(newElementsList));
        assertEquals("Invalid gigaSet size", ++expectedSize, gigaSpace.count(null));
        
        Queue<T> newElementsQueue = new LinkedList<>(newElementsList);
        newElementsQueue.offer(newNotNullElement());
        newElementsQueue.offer(newNotNullElement());
        newElementsQueue.offer(newNotNullElement());
        expectedSize += 3;
        assertTrue("GigaSet should be changed after adding new elements from queue = " + newElementsQueue, gigaSet.addAll(newElementsQueue));
        assertEquals("Invalid gigaSet size", expectedSize, gigaSpace.count(null));
        
        Set<T> newElementsSet = new HashSet<>(testElements);
        newElementsSet.add(newNotNullElement());
        assertTrue("GigaSet should be changed after adding existing and new elements from set = " + newElementsSet, gigaSet.addAll(newElementsSet));
        assertEquals("Invalid gigaSet size", ++expectedSize, gigaSpace.count(null));
    }
    
    @Test
    public void testClear() {
        gigaSet.clear();
        
        assertEquals("Invalid gigaSet size", 0, gigaSpace.count(null));
    }
    
    @Test(expected = NullPointerException.class)
    public void testRemoveNull() {
        gigaSet.remove(null);
    }
    
    @Test
    public void testRemove() {
        int expectedSize = testElements.size();
        if (!testElements.isEmpty()) {
            T element = testElements.iterator().next();
            assertTrue("GigaSet should return true after removing an existing element = " + element, gigaSet.remove(element));
            assertEquals("Invalid gigaSet size", --expectedSize, gigaSpace.count(null));
        }
        
        T newElement = newNotNullElement();
        assertFalse("GigaSet should return true after removing an unexisting element = " + newElement, gigaSet.remove(newElement));
        assertEquals("Invalid gigaSet size", expectedSize, gigaSpace.count(null));
        
       /* Object newType = new Serializable() {};
        assertFalse("GigaSet should return true after removing an unexisting element = " + newType, gigaSet.remove(newType));
        assertEquals("Invalid gigaSet size", expectedSize, gigaSpace.count(null));*/
    }
    
    @Test(expected = NullPointerException.class)
    public void testRemoveAllNull() {
        gigaSet.removeAll(null);
    }
    
    @Test(expected = NullPointerException.class)
    public void testRemoveAllNullElement() {
        gigaSet.removeAll(Arrays.asList(null));
    }
    
    @Test
    public void testRemoveAll() {
        assertFalse("GigaSet should not be changed after removing empty set", gigaSet.removeAll(Collections.emptySet()));
        int expectedSize = testElements.size();
        assertEquals("Invalid gigaSet size", expectedSize, gigaSpace.count(null));
        
        List<T> newElementsList = Collections.singletonList(newNotNullElement());
        assertFalse("GigaSet should not be changed after removing list with unexisting elements", gigaSet.removeAll(newElementsList));
        
        Set<T> subSet = getTestDataSubSet();
        expectedSize -= subSet.size();
        subSet.add(newNotNullElement());
        subSet.add(newNotNullElement());
        
        if (!testElements.isEmpty()) {
            assertTrue("GigaSet should be changed after removing set by collection with existing and unexisting elements", gigaSet.removeAll(subSet));
        } else {
            assertFalse("GigaSet should not be changed after removing some elements from empty set", gigaSet.removeAll(subSet));
        }
        
        assertEquals("Invalid gigaSet size", expectedSize, gigaSpace.count(null));
    }
    
    @Test
    public void testRemoveAllElements() {
        if (!testElements.isEmpty()) {
            assertTrue("GigaSet should be changed after removing all its elements", gigaSet.removeAll(testElements));
            assertEquals("Invalid gigaSet size", 0, gigaSpace.count(null));
        }
    }
    
    @Test(expected = NullPointerException.class)
    public void testRetainAllNull() {
        gigaSet.retainAll(null);
    }
    
    @Test(expected = NullPointerException.class)
    public void testRetainAllNullElement() {
        gigaSet.retainAll(Arrays.asList(null));
    }
    
    @Test
    public void testRetainAll() {
        assertFalse("GigaSet should not be changed after retaining all its elements", gigaSet.retainAll(testElements));
        assertEquals("Invalid gigaSet size", testElements.size(), gigaSpace.count(null));
        
        Set<T> subSet = getTestDataSubSet();
        if (subSet.isEmpty() || testElements.size() == subSet.size()) {
            assertFalse("GigaSet should not be changed after retaining all its elements", gigaSet.retainAll(subSet));
        } else {
            assertTrue("GigaSet should be changed after retaining a subset of its elements", gigaSet.retainAll(subSet));
        }
        assertEquals("Invalid gigaSet size", subSet.size(), gigaSpace.count(null));
    }
    
    @Test
    public void testRetainAllEmptyCollection() {
        if (!testElements.isEmpty()) {
            assertTrue("GigaSet should be changed after retaining an empty collection", gigaSet.retainAll(Collections.emptyList()));
            assertEquals("Invalid gigaSet size", 0, gigaSpace.count(null));
        }
    }
    
    protected abstract T newNotNullElement();
    
    private Set<T> getTestDataSubSet() {
        Set<T> subSet = new HashSet<>();
        int i = 0;
        for (T e : testElements) {
            if (i ++ % 2 == 0) {
                subSet.add(e);
            }
        }
        return subSet;
    }
}
