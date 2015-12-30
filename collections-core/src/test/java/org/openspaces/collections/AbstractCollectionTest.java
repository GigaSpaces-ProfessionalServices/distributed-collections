package org.openspaces.collections;

import org.junit.Assume;
import org.junit.Test;
import org.openspaces.core.GigaSpace;
import org.springframework.test.context.TestContextManager;

import javax.annotation.Resource;
import java.util.*;

import static org.junit.Assert.*;

public abstract class AbstractCollectionTest<T> {

    @Resource
    protected GigaSpace gigaSpace;

    protected List<T> testedElements;

    protected abstract Collection<T> getCollection();

    protected AbstractCollectionTest(List<T> elements) {
        this.testedElements = elements;
        setUpSpringContext();
    }

    private void setUpSpringContext() {
        try {
            new TestContextManager(getClass()).prepareTestInstance(this);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to set spring context up", e);
        }
    }

    @Test(expected = NullPointerException.class)
    public void testAddAllNull() {
        getCollection().addAll(null);
    }

    @Test(expected = NullPointerException.class)
    public void testRemoveAllNull() {
        getCollection().removeAll(null);
    }

    @Test(expected = NullPointerException.class)
    public void testContainsAllNull() {
        getCollection().containsAll(null);
    }

    @Test(expected = NullPointerException.class)
    public void testRetainAllNull() {
        getCollection().retainAll(null);
    }

    @Test(expected = NullPointerException.class)
    public void testAddAllWithNull() throws InterruptedException {
        List<T> list = new ArrayList<>();
        list.add(newElement());
        list.add(null);
        getCollection().addAll(list);
    }

    @Test(expected = NullPointerException.class)
    public void testRemoveAllWithNull() throws InterruptedException {
        List<T> list = new ArrayList<>();
        list.add(newElement());
        list.add(null);
        getCollection().addAll(list);
    }

    @Test
    public void testClear() {
        getCollection().clear();

        assertSize("Invalid collection size", 0);
    }

    @Test
    public void testContains() {
        final Collection<T> collection = getCollection();
        if (!testedElements.isEmpty()) {
            T element = testedElements.iterator().next();
            assertTrue("Collection should contain the element = " + element, collection.contains(element));
        }

        T element = newElement();
        assertFalse("Collection should not contain the element = " + element, collection.contains(element));

      /*  Object newType = new Serializable() {};
        assertFalse("Collection should not contain the element = " + newType, gigaSet.contains(newType));*/
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

        Set<T> newElementsSet = Collections.singleton(newElement());
        assertFalse("Collection should not contain the elements from the collection = " + newElementsSet, collection.containsAll(newElementsSet));

        List<T> newElementsList = Collections.singletonList(newElement());
        assertFalse("Collection should not contain the elements from the collection = " + newElementsList, collection.containsAll(newElementsList));

        Set<T> elementsSet = new HashSet<>(testedElements);
        elementsSet.add(newElement());
        assertFalse("Collection should not contain the elements from the collection = " + elementsSet, collection.containsAll(elementsSet));

        Queue<T> elementsQueue = new LinkedList<>(testedElements);
        elementsQueue.offer(newElement());
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

        T element = newElement();
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
        while (iterator.hasNext()) {
            assertNotNull("Collection elements must not be null", iterator.next());
            actualCount++;
        }
        assertSize("Invalid collection elements count", actualCount);
    }

    @Test
    public void testRemove() {
        int expectedSize = testedElements.size();
        final Collection<T> collection = getCollection();
        if (!testedElements.isEmpty()) {
            T element = testedElements.iterator().next();
            assertTrue("Collection should return true after removing an existing element = " + element, collection.remove(element));
            assertSize("Invalid collection size", --expectedSize);
        }

        T newElement = newElement();
        assertFalse("Collection should return true after removing an unexisting element = " + newElement, collection.remove(newElement));
        assertSize("Invalid collection size", expectedSize);
        
       /* Object newType = new Serializable() {};
        assertFalse("GigaSet should return true after removing an unexisting element = " + newType, gigaSet.remove(newType));
        assertEquals("Invalid gigaSet size", expectedSize, gigaSpace.count(null));*/
    }

    @Test
    public void testRemoveAll() {
        final Collection<T> collection = getCollection();
        assertFalse("Collection should not be changed after removing empty set", collection.removeAll(Collections.emptySet()));
        int expectedSize = testedElements.size();
        assertSize("Invalid collection size", expectedSize);

        List<T> newElementsList = Collections.singletonList(newElement());
        assertFalse("Collection should not be changed after removing list with unexisting elements", collection.removeAll(newElementsList));

        Collection<T> subCollection = getTestedDataSubCollection();
        expectedSize -= subCollection.size();
        subCollection.add(newElement());
        subCollection.add(newElement());

        if (!testedElements.isEmpty()) {
            assertTrue("Collection should be changed after removing a collection with existing and unexisting elements", collection.removeAll(subCollection));
        } else {
            assertFalse("Collection should not be changed after removing some elements from empty collection", collection.removeAll(subCollection));
        }

        assertSize("Invalid collection size", expectedSize);
    }

    @Test
    public void testRemoveAllElements() {
        Assume.assumeFalse(testedElements.isEmpty());

        assertTrue("Collection should be changed after removing all its elements", getCollection().removeAll(testedElements));
        assertSize("Invalid collection size", 0);
    }

    @Test
    public void testRetainAll() {
        final Collection<T> collection = getCollection();
        assertFalse("Collection should not be changed after retaining all its elements", collection.retainAll(testedElements));
        assertSize("Invalid collection size", testedElements.size());

        Collection<T> subCollection = getTestedDataSubCollection();
        if (subCollection.isEmpty() || testedElements.size() == subCollection.size()) {
            assertFalse("Collection should not be changed after retaining all its elements", collection.retainAll(subCollection));
        } else {
            assertTrue("Collection should be changed after retaining a sub collection of its elements", collection.retainAll(subCollection));
        }
        assertSize("Invalid collection size", subCollection.size());
    }

    @Test
    public void testRetainAllEmptyCollection() {
        Assume.assumeFalse(testedElements.isEmpty());

        assertTrue("Collection should be changed after retaining an empty collection", getCollection().retainAll(Collections.emptyList()));
        assertSize("Invalid collection size", 0);
    }

    @Test
    public void testSize() {
        int expectedSize = testedElements.size();
        final Collection<T> collection = getCollection();
        assertEquals("Invalid collection size", expectedSize, collection.size());

        T element = newElement();
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
        assertSize("Invalid collection elements count", objectArray.length);
        for (Object o : objectArray) {
            assertNotNull("Collection elements must not be null", o);
            assertTrue("Invalid element type", o.getClass().isAssignableFrom(getElementType()));
        }

        T[] elementArray = collection.toArray(getElementArray());
        assertNotNull("Array of collection elements must not be null", elementArray);
        assertSize("Invalid collection elements count", elementArray.length);
        for (T o : elementArray) {
            assertNotNull("Collection elements must not be null", o);
        }
    }

    protected abstract void assertSize(String msg, int expected);

    protected abstract Class<? extends T> getElementType();

    protected abstract T[] getElementArray();

    protected Collection<T> getTestedDataSubCollection() {
        Collection<T> subSet = new HashSet<>();
        int i = 0;
        for (T e : testedElements) {
            if (i++ % 2 == 0) {
                subSet.add(e);
            }
        }
        return subSet;
    }

    protected abstract T newElement();
}
