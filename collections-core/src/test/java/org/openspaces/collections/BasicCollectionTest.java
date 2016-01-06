package org.openspaces.collections;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.Test;

import java.util.*;

import static org.testng.Assert.*;

public abstract class BasicCollectionTest extends AbstractTestNGSpringContextTests {
    private static final Logger LOG = LoggerFactory.getLogger(BasicCollectionTest.class);

    protected abstract List<Object> getContent();

    protected abstract Collection<Object> getCollection();

    protected abstract Object newElement();

    protected abstract Class<?> getElementType();

    protected abstract Object[] getElementArray();

    protected abstract void assertSize(int expected, String message);

    @Test(groups = "all", expectedExceptions = NullPointerException.class)
    public void testAddAllNull() {
        getCollection().addAll(null);
    }

    @Test(groups = "all", expectedExceptions = NullPointerException.class)
    public void testRemoveAllNull() {
        getCollection().removeAll(null);
    }

    @Test(groups = "all", expectedExceptions = NullPointerException.class)
    public void testContainsAllNull() {
        getCollection().containsAll(null);
    }

    @Test(groups = "all", expectedExceptions = NullPointerException.class)
    public void testRetainAllNull() {
        getCollection().retainAll(null);
    }

    @Test(groups = "all", expectedExceptions = NullPointerException.class)
    public void testAddAllWithNull() throws InterruptedException {
        List<Object> list = new ArrayList<>();
        list.add(newElement());
        list.add(null);
        getCollection().addAll(list);
    }

    @Test(groups = "all", expectedExceptions = NullPointerException.class)
    public void testRemoveAllWithNull() throws InterruptedException {
        List<Object> list = new ArrayList<>();
        list.add(newElement());
        list.add(null);
        getCollection().addAll(list);
    }

    @Test(groups = "all")
    public void testClear() {
        getCollection().clear();
        assertSize(0);
    }

    @Test(groups = "all")
    public void testContains() {
        Collection<Object> collection = getCollection();
        if (!getContent().isEmpty()) {
            Object element = getContent().iterator().next();
            assertTrue(collection.contains(element), "Collection should contain the element = " + element);
        }

        Object element = newElement();
        assertFalse(collection.contains(element), "Collection should not contain the element = " + element);
    }

    @Test(groups = "all")
    public void testContainsAll() {
        Collection<Object> collection = getCollection();
        assertTrue(collection.containsAll(Collections.emptySet()), "Collection should contain empty set");
        assertTrue(collection.containsAll(Collections.emptyList()), "Collection should contain empty list");

        assertTrue(collection.containsAll(getContent()), "Collection should contain all its elements");
        assertTrue(collection.containsAll(new ArrayList<>(getContent())), "Collection should contain all its elements");

        Collection<Object> subCollection = getContentSubCollection();
        assertTrue(collection.containsAll(subCollection), "Collection should contain subset of its elements");
        assertTrue(collection.containsAll(new ArrayList<>(subCollection)), "Collection should contain subset of its elements");

        Set<Object> newElementsSet = Collections.singleton(newElement());
        assertFalse(collection.containsAll(newElementsSet), "Collection should not contain the elements from the collection = " + newElementsSet);

        List<Object> newElementsList = Collections.singletonList(newElement());
        assertFalse(collection.containsAll(newElementsList), "Collection should not contain the elements from the collection = " + newElementsList);

        Set<Object> elementsSet = new HashSet<>(getContent());
        elementsSet.add(newElement());
        assertFalse(collection.containsAll(elementsSet), "Collection should not contain the elements from the collection = " + elementsSet);

        Queue<Object> elementsQueue = new LinkedList<>(getContent());
        elementsQueue.offer(newElement());
        assertFalse(collection.containsAll(elementsQueue), "Collection should not contain the elements from the collection = " + elementsQueue);
    }

    @Test(groups = "all")
    public void testIsEmpty() {
        Collection<Object> collection = getCollection();
        if (!getContent().isEmpty()) {
            assertFalse(collection.isEmpty(), "Collection should not be empty");
        } else {
            assertTrue(collection.isEmpty(), "Collection should be empty");
        }

        Object element = newElement();
        collection.add(element);
        assertFalse(collection.isEmpty(), "Collection should not be empty");

        collection.remove(element);
        if (!getContent().isEmpty()) {
            assertFalse(collection.isEmpty(), "Collection should not be empty");
        } else {
            assertTrue(collection.isEmpty(), "Collection should be empty");
        }

        collection.clear();
        assertTrue(collection.isEmpty(), "Collection should be empty");
    }

    @Test(groups = "all")
    public void testIterator() {
        Iterator<Object> iterator = getCollection().iterator();
        assertNotNull(iterator, "Iterator must not be null");
        int actualCount = 0;
        while (iterator.hasNext()) {
            assertNotNull(iterator.next(), "Collection elements must not be null");
            actualCount++;
        }
        assertSize(actualCount, "Invalid collection elements count");
    }

    @Test(groups = "all")
    public void testRemove() {
        int expectedSize = getContent().size();
        Collection<Object> collection = getCollection();
        if (!getContent().isEmpty()) {
            Object element = getContent().iterator().next();
            assertTrue(collection.remove(element), "Collection should return true after removing an existing element = " + element);
            assertSize(--expectedSize);
        }

        Object newElement = newElement();
        assertFalse(collection.remove(newElement), "Collection should return false after removing an unexisting element = " + newElement);
        assertSize(expectedSize);
    }

    @Test(groups = "all")
    public void testRemoveAll() {
        Collection<Object> collection = getCollection();
        assertFalse(collection.removeAll(Collections.emptySet()), "Collection should not be changed after removing empty set");
        int expectedSize = getContent().size();
        assertSize(expectedSize);

        List<Object> newElementsList = Collections.singletonList(newElement());
        assertFalse(collection.removeAll(newElementsList), "Collection should not be changed after removing list with unexisting elements");
        assertSize(expectedSize);

        Collection<Object> subCollection = getContentSubCollection();
        expectedSize -= subCollection.size();
        subCollection.add(newElement());
        subCollection.add(newElement());

        if (!getContent().isEmpty()) {
            assertTrue(collection.removeAll(subCollection), "Collection should be changed after removing a collection with existing and unexisting elements");
        } else {
            assertFalse(collection.removeAll(subCollection), "Collection should not be changed after removing some elements from empty collection");
        }

        assertSize(expectedSize);
    }

    @Test(groups = "filled")
    public void testRemoveAllElements() {
        assertTrue(getCollection().removeAll(getContent()), "Collection should be changed after removing all its elements");
        assertSize(0);
    }

    @Test(groups = "all")
    public void testRetainAll() {
        Collection<Object> collection = getCollection();
        assertFalse(collection.retainAll(getContent()), "Collection should not be changed after retaining all its elements");
        assertSize(getContent().size(), "Invalid collection size");

        Collection<Object> subCollection = getContentSubCollection();
        if (subCollection.isEmpty() || getContent().size() == subCollection.size()) {
            assertFalse(collection.retainAll(subCollection), "Collection should not be changed after retaining all its elements");
        } else {
            assertTrue(collection.retainAll(subCollection), "Collection should be changed after retaining a sub collection of its elements");
        }
        assertSize(subCollection.size());
    }

    @Test(groups = "filled")
    public void testRetainAllEmptyCollection() {
        assertTrue(getCollection().retainAll(Collections.emptyList()), "Collection should be changed after retaining an empty collection");
        assertSize(0);
    }

    @Test(groups = "all")
    public void testSize() {
        int expectedSize = getContent().size();
        Collection<Object> collection = getCollection();
        assertEquals(collection.size(), expectedSize, "Invalid collection size");

        Object element = newElement();
        collection.add(element);
        assertEquals(collection.size(), ++expectedSize, "Invalid collection size");

        collection.remove(element);
        assertEquals(collection.size(), --expectedSize, "Invalid collection size");

        collection.clear();
        assertEquals(collection.size(), 0, "Invalid collection size");
    }

    @Test(groups = "all")
    public void testToArray() {
        Collection<Object> collection = getCollection();
        Object[] objectArray = collection.toArray();
        assertNotNull(objectArray, "Array of collection elements must not be null");
        assertSize(objectArray.length, "Invalid collection elements count");
        for (Object element : objectArray) {
            assertNotNull(element, "Collection elements must not be null");
            assertTrue(element.getClass().isAssignableFrom(getElementType()), "Invalid element elementType");
        }

        Object[] elementArray = collection.toArray(getElementArray());
        assertNotNull(elementArray, "Array of collection elements must not be null");
        assertSize(elementArray.length, "Invalid collection elements count");
        for (Object object : elementArray) {
            assertNotNull(object, "Collection elements must not be null");
        }
    }

    protected void assertSize(int expected) {
        assertSize(expected, "Invalid collection size");
    }

    protected List<Object> newElements(int count) {
        List<Object> objects = new ArrayList<>(count);
        for (int index = 0; index < count; index++) {
            objects.add(newElement());
        }
        return objects;
    }

    protected Collection<Object> getContentSubCollection() {
        Collection<Object> subSet = new HashSet<>();
        int index = 0;
        for (Object element : getContent()) {
            if (index++ % 2 == 0) {
                subSet.add(element);
            }
        }
        return subSet;
    }

}
